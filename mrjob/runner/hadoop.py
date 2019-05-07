# -*- coding: utf-8 -*-

import argparse
import glob
import io
import logging
import pipes
import os
import re
import subprocess
import sys


PYTHON_ARCHIVE = 'afs://tianqi.afs.baidu.com:9902/user/ubs/pv/common/python2.7.tar.gz#python2.7.1'
PYTHON_EXEC = 'python2.7.1/python/bin/python'

HADOOP_TIANQI = '/home/work/hadoop-client-yq/hadoop/bin/hadoop'
HADOOP_BIN = HADOOP_TIANQI

QUEUE_MAPPER = {
    'tianqi-ubs-pv': {
        'mapred.job.queue.name': 'tianqi-ubs-pv',
        'mapred.job.tracker': 'yq01-tianqi-job.dmop.baidu.com:54311',
    },
    'xingtian-ubs-pv': {
        'mapred.job.queue.name': 'xingtian-ubs-pv',
        'mapred.job.tracker': 'yq01-xingtian-job.dmop.baidu.com:54311',
    },
    'ubs-pv-chunjie': {
        'mapred.job.queue.name': 'ubs-pv-chunjie',
        'mapred.job.tracker': 'yq01-xingtian-job.dmop.baidu.com:54311',
    },
}

_HADOOP_RM_NO_SUCH_FILE = re.compile(r'\nrmr?: .*No such file.*\n')

logger = logging.getLogger('mrjob')


def set_hadoop_python(python_archive, python_exec):
    """set python archive and python executable for hadoop streaming.

    `python_archive` will be passed to hadoop streaming by -cacheArchive option.
    `python_exec` is python executable path, which will be used to execute
        python script on hadoop cluster.
    """
    global PYTHON_ARCHIVE, PYTHON_EXEC

    PYTHON_ARCHIVE = python_archive
    PYTHON_EXEC = python_exec


def _invoke_hadoop(cmd, ok_returncodes=None, ok_stderr=None, return_stdout=False):
    """包装调用 hadoop 客户端的命令，并屏蔽指定的 stderr"""

    logger.info('> {}'.format(' '.join(cmd)))
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = proc.communicate()

    # check if STDERR is okay
    stderr_is_ok = False
    if ok_stderr:
        for stderr_re in ok_stderr:
            if stderr_re.search(stderr):
                stderr_is_ok = True
                break

    if not stderr_is_ok:
        for line in io.BytesIO(stderr):
            # skip HDFS info in stderr stream
            if ' INFO ' in line:
                continue
            logger.error('STDERR: ' + line.rstrip())

    ok_returncodes = ok_returncodes or [0]

    if not stderr_is_ok and proc.returncode not in ok_returncodes:
        raise subprocess.CalledProcessError(proc.returncode, cmd)

    if return_stdout:
        return stdout
    else:
        if stdout:
            logger.info('STDOUT: ' + stdout)
        return proc.returncode


class HadoopError(Exception): pass


class HadoopRunner(object):
    """hadoop runner for MRJob.
    Run Map-Reduce job with hadoop streaming.
    """

    ALL_OPTS = {
        'hadoop',
        'input', # single string or list of strings
        'output',
        'cmdenv', # dict
        'cacheArchive', # single or list
        'file', # single or list
        'mapper', 'combiner', 'reducer',
        'inputformat', 'outputformat',
        'partitioner',
        'others', # other opts in a list, will be passed to command line
        'merge_output', # merge output files as specific numbers
    }

    DEFAULT_OPTS = {
        'hadoop': HADOOP_BIN,
    }

    DEFAULT_JOBCONF = {
        'mapred.job.queue.name': 'tianqi-ubs-pv',
        'mapred.job.priority': 'NORMAL',
        'mapred.job.map.capacity': 4000,
        'mapred.job.reduce.capacity': 800,
    }

    REQUIRED_OPTS = {'input', 'output'}


    def __init__(self, mrjob, cmd_args=None, **kwargs):
        self.mrjob = mrjob

        jobconf = kwargs.pop('jobconf', {})
        options = kwargs

        # 如果有命令行参数，将会覆盖脚本中传入的参数
        if cmd_args is not None:
            cmd_options, cmd_jobconf = self._parse_cmd_args(cmd_args)
            options.update(cmd_options)
            jobconf.update(cmd_jobconf)

        options = self._validate_options(options)

        self._options = dict(self.DEFAULT_OPTS)
        self._options.update(self._default_mr_options())
        self._options.update(options)

        self._jobconf = dict(self.DEFAULT_JOBCONF)
        self._jobconf.update(jobconf)

        # 根据设定的队列自动补全其他必要的配置
        queue_name = self._jobconf['mapred.job.queue.name']
        if queue_name in QUEUE_MAPPER:
            self._jobconf.update(QUEUE_MAPPER[queue_name])


    def _parse_cmd_args(self, cmd_args):
        """parse command line arguments (currently only -D options)"""
        options = {}
        jobconf = {}

        parser = argparse.ArgumentParser()
        parser.add_argument(
            '-input', dest='input', action='append',
            help='Input location for mapper. The same as `hadoop streaming -input`.')
        parser.add_argument(
            '-output', dest='output',
            help='Output location for reducer. The same as `hadoop streaming -output`.')
        parser.add_argument(
            '-mapper', dest='mapper', help='Mapper executable. If not specified, IdentityMapper is used as the default. The same as `hadoop streaming -mapper`.')
        parser.add_argument(
            '-combiner', dest='combiner', help='Combiner executable for map output. The same as `hadoop streaming -combiner`.')
        parser.add_argument(
            '-reducer', dest='reducer', help='Reducer executable. If not specified, IdentityReducer is used as the default. The same as `hadoop streaming -reducer`.')
        parser.add_argument(
            '-D', '--jobconf', dest='jobconf', action='append', default=[],
            help='Use value for given property. The same as `hadoop streaming -D`.')

        args = parser.parse_args(cmd_args)

        # parse options
        for name in ('input', 'output', 'mapper', 'combiner', 'reducer'):
            if getattr(args, name, None):
                options[name] = getattr(args, name)

        # parse jobconf
        for s in args.jobconf:
            if not re.match(r'.+=.+', s):
                logger.warning('Invalid command line option: "-D {}"'.format(s))
                continue
            key, value = s.split('=', 1)
            jobconf[key] = value

        return options, jobconf


    def _validate_options(self, options):
        """check and modify options"""
        logger.info('checking job config ...')

        for k in options:
            if k not in self.ALL_OPTS:
                logger.warning('unknown option "{}={}", ignored.'.format(k, options[k]))

        # check required options
        for name in self.REQUIRED_OPTS:
            if name not in options:
                raise ValueError('option "{}" is required'.format(name))

        # check input
        if isinstance(options['input'], basestring):
            options['input'] = [options['input']]
        elif isinstance(options['input'], (list, tuple)):
            if options['input'] and not isinstance(options['input'][0], basestring):
                raise ValueError('option "input" should be a string or a sequence of strings')
        else:
            raise ValueError('option "input" should be a string or a sequence of strings')

        # check output
        if not isinstance(options['output'], basestring):
            raise ValueError('option "output" should be a string')

        # check cacheArchive
        if 'cacheArchive' in options:
            if isinstance(options['cacheArchive'], basestring):
                options['cacheArchive'] = [options['cacheArchive']]
            elif isinstance(options['cacheArchive'], (list, tuple)):
                if options['cacheArchive'] and not isinstance(options['cacheArchive'][0], basestring):
                    raise ValueError('option "cacheArchive" should be a string or a sequence of strings')
        else:
            options['cacheArchive'] = []

        # check cmdenv
        if 'cmdenv' in options:
            try:
                options['cmdenv'] = dict(options['cmdenv'])
            except ValueError as e:
                raise ValueError('option "cmdenv" should be a dict-like object')
        else:
            options['cmdenv'] = {}

        # check file
        if 'file' in options:
            if isinstance(options['file'], basestring):
                options['file'] = [options['file']]
            elif isinstance(options['file'], (list, tuple)):
                if options['file'] and not isinstance(options['file'][0], basestring):
                    raise ValueError('option "file" should be a string or a sequence of strings')
            else:
                raise ValueError('option "file" should be a string or a sequence of strings')

            for file in set(options['file']):
                if not glob.glob(file):
                    raise ValueError('Invalid option "file": path "{}" not exist'.format(file))
        else:
            options['file'] = []

        # check mapper/combiner/reducer
        for name in ('mapper', 'combiner', 'reducer'):
            if name not in options:
                continue
            if not isinstance(options[name], basestring):
                raise ValueError('option "{}" should be a string'.format(name))
            if options[name].startswith('python '):
                options[name] = options[name].replace('python', PYTHON_EXEC, 1)

        # check others
        if 'others' in options:
            if not isinstance(options['others'], (list, tuple)):
                raise ValueError('option "others" should be a sequence of strings')
        else:
            options['others'] = []

        logger.info('job config OK.')
        return options

    def _default_mr_options(self):
        """get default -mapper/-combiner/-reducer options"""
        res = {}
        py_script = os.path.split(sys.argv[0])[-1]
        for name in ('mapper', 'combiner', 'reducer'):
            if not self.mrjob._has_mr_fun(name):
                continue
            res[name] = '{python} "{script}" --{name}'.format(
                name=name, python=PYTHON_EXEC, script=py_script)
        return res

    def _generate_cmd(self):
        """generate hadoop streaming command"""
        cmd = [self._options['hadoop'], 'streaming']

        # set default job name as current python script name
        py_script = sys.argv[0]
        if 'mapred.job.name' not in self._jobconf:
            self._jobconf['mapred.job.name'] = 'mrjob-{}'.format(py_script)

        for k, v in self._jobconf.items():
            cmd.extend(['-D', '{}={}'.format(k, v)])

        for key in ('inputformat', 'outputformat', 'partitioner'):
            if key in self._options:
                cmd.extend(['-' + key, self._options[key]])

        for path in set(self._options['input']):
            cmd.extend(['-input', path])

        cmd.extend(['-output', self._options['output']])

        for k, v in self._options['cmdenv'].items():
            cmd.extend(['-cmdenv', '{}={}'.format(k, v)])

        for archive in set(self._options['cacheArchive'] + [PYTHON_ARCHIVE]):
            cmd.extend(['-cacheArchive', archive])

        cur_dir = os.path.dirname(os.path.abspath(__file__))
        mrjob_py = os.path.join(os.path.dirname(cur_dir), 'bundle', 'mrjob.py')
        if not os.path.isfile(mrjob_py):
            bundle()

        for file in set(self._options['file'] + [py_script, mrjob_py]):
            cmd.extend(['-file', file])

        # if not self.mrjob._has_mr_fun('mapper'):
        #     raise ValueError('You have to implement the "mapper" method')

        for name in ('mapper', 'combiner', 'reducer'):
            if name in self._options:
                cmd.extend(['-' + name, self._options[name]])

        cmd.extend(self._options['others'])

        return cmd


    def _pretty_cmd(self, cmd):
        """get pretty looking command (for print)"""
        cmd = cmd[:]
        placeholder = '-__indent__'
        for i, op in enumerate(cmd):
            if op.startswith('-'):
                cmd[i] = placeholder + op
        shell_cmd = ' '.join(pipes.quote(s) for s in cmd)
        shell_cmd = shell_cmd.replace(placeholder, ' \\\n\t')
        return shell_cmd


    def execute(self):
        """execute hadoop streaming command.

        If success, then overwrite output directory; therwise, leave output
        directory untouched.
        This feature is learnt from hive(hql), which avoids mistakenly deleting
        current data in case of the job will fail.
        """

        # 使用一个临时目录保存结果
        output_tmp = self._options['output'].rstrip('/') + '__tmp_mrjob'
        rm_tmp = [self._options['hadoop'], 'fs', '-rmr', output_tmp]
        _invoke_hadoop(rm_tmp, ok_stderr=[_HADOOP_RM_NO_SUCH_FILE])

        cmd = self._generate_cmd()
        cmd[cmd.index('-output')+1] = output_tmp
        logger.info('\n' + self._pretty_cmd(cmd) + '\n')

        logger.info('running hadoop streaming ...')
        # 执行 hadoop streaming 命令，打印 stdout, stderr 到父进程的 stdout, stderr
        retcode = subprocess.call(cmd, stdout=None, stderr=None)

        # 如果作业成功，先删除 output 目录，然后将临时目录 move 到 output 目录。
        if retcode == 0:
            rm_output = [self._options['hadoop'], 'fs', '-rmr', self._options['output']]
            _invoke_hadoop(rm_output, ok_stderr=[_HADOOP_RM_NO_SUCH_FILE])

            # merge small output files if needed
            merge_flag = ('merge_output' in self._options
                and self._options['merge_output'] < self._jobconf.get('mapred.reduce.tasks', 0))

            # move tmp_output to output
            cmd_mv = [self._options['hadoop'], 'fs', '-mv', output_tmp, self._options['output']]

            if not merge_flag:
                _invoke_hadoop(cmd_mv)
            else:
                cmd_merge = [
                    self._options['hadoop'], 'streaming',
                    '-D', 'mapred.job.queue.name={}'.format(self._jobconf['mapred.job.queue.name']),
                    '-D', 'mapred.job.tracker={}'.format(self._jobconf['mapred.job.tracker']),
                    '-D', 'mapred.reduce.tasks={}'.format(self._options['merge_output']),
                    '-input', output_tmp, '-output', self._options['output'], '-mapper', 'cat', ]
                sys.stderr.write('\n\n')
                logger.info('merging output files ...')
                logger.info('\n' + self._pretty_cmd(cmd_merge) + '\n')
                retcode = subprocess.call(cmd_merge, stdout=None, stderr=None)
                if retcode != 0:
                    try:
                        _invoke_hadoop(cmd_mv)
                    except subprocess.CalledProcessError:
                        raise HadoopError('Failed moving tmp_output to output')
                    finally:
                        raise HadoopError('Failed merging output files')
                rm_tmp = [self._options['hadoop'], 'fs', '-rmr', output_tmp]
                _invoke_hadoop(rm_tmp, ok_stderr=[_HADOOP_RM_NO_SUCH_FILE])

            logger.info('final output: {}'.format(self._options['output']))

        # 如果作业失败，保持 output 目录不变，仅删除临时目录
        else:
            logger.error('hadoop streaming failed.')
            rm_tmp = [self._options['hadoop'], 'fs', '-rmr', output_tmp]
            _invoke_hadoop(rm_tmp, ok_stderr=[_HADOOP_RM_NO_SUCH_FILE])
            raise HadoopError(
                'hadoop streaming command returned non-zero exit status {}. '
                'Job quited without touching output directory. \nTo debug, please '
                'follow the Tracking URL and see "stderr" or "hce.userlog".'
                .format(retcode))


if __name__ == '__main__':
    pass


def bundle():
    """bundle all python scripts of mrjob into one, so that is could be loaded
    by hadoop streaming through `-file` option.

    every time after you edited the code of mrjob, make sure this function to
    be called once, or `mrjob.py` loaded by hadoop streaming will remain unchanged.
    """
    MODULE_NAMES = (r'\.', r'\.\.', 'job', 'protocol', 'util', 'hadoop', 'local')

    RE_MAIN = re.compile(r'^if +__name__ *== *[\'\"]__main__[\'\"] *:')
    RE_IMPORT = re.compile(r'import +([\._a-zA-Z]*\.)*({})'.format('|'.join(MODULE_NAMES)))
    RE_FROM_IMPORT = re.compile(r'from +([\._a-zA-Z]*\.)*({}) +import'.format('|'.join(MODULE_NAMES)))

    cur_dir = os.path.dirname(os.path.abspath(__file__))
    root_dir = os.path.dirname(cur_dir)
    runner_dir = cur_dir

    out_dir = os.path.join(root_dir, 'bundle')
    if not os.path.isdir(out_dir):
        os.makedirs(out_dir)
    out_file = os.path.join(out_dir, 'mrjob.py')

    with open(out_file, 'wb') as fout:
        fout.write(b'# -*- coding: utf-8 -*-\n\n')

        for file in (
                os.path.join(root_dir, 'job.py'),
                os.path.join(root_dir, 'protocol.py'),
                os.path.join(root_dir, 'util.py'),
                os.path.join(runner_dir, 'hadoop.py'),
                os.path.join(runner_dir, 'local.py'),
                ):
            fout.write(b'# ' + file + b'\n')

            with open(file, 'rb') as fin:
                for line in fin:
                    if RE_MAIN.search(line):
                        break
                    if RE_IMPORT.search(line):
                        continue
                    if RE_FROM_IMPORT.search(line):
                        continue
                    fout.write(line)


if __name__ == '__main__':
    bundle()

# -*- coding: utf-8 -*-

import argparse
import itertools
import logging
import os
from operator import itemgetter
import sys

from runner.hadoop import HadoopRunner
from runner.local import LocalRunner
from protocol import TextValueProtocol, PickleProtocol
from util import flatten


logger = logging.getLogger('mrjob')
formatter = logging.Formatter('[%(asctime)s] %(name)s %(levelname)s: %(message)s')
handler = logging.StreamHandler()
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.propagate = False


class MRJob(object):
    """map-reducer job base class"""

    def __init__(self):
        # always read and write bytes, instead of unicodes
        # sys.stdin.buffer in Python3 acts like sys.stdin in Python2
        self._stdin = getattr(sys.stdin, 'buffer', sys.stdin)
        self._stdout = getattr(sys.stdout, 'buffer', sys.stdout)
        self._stderr = getattr(sys.stderr, 'buffer', sys.stderr)

        self.input_protocol = TextValueProtocol()
        self.internal_protocol = PickleProtocol()
        self.output_protocol = TextValueProtocol()

        # enable logging if user haven't
        logging.basicConfig(level=logging.INFO)

    # you can implement these methods
    # -----------------------------------------------------------
    # def mapper(self, key, value):
    #     """for end users to implement"""
    #     raise NotImplementedError

    # def combiner(self, key, values):
    #     """for end users to implement"""
    #     raise NotImplementedError

    # def reducer(self, key, values):
    #     """for end users to implement"""
    #     raise NotImplementedError

    # def mapper_init(self):
    #     raise NotImplementedError

    # def combiner_init(self):
    #     raise NotImplementedError

    # def reducer_init(self):
    #     raise NotImplementedError

    # def mapper_final(self):
    #     raise NotImplementedError

    # def combiner_final(self):
    #     raise NotImplementedError

    # def reducer_final(self):
    #     raise NotImplementedError
    # -----------------------------------------------------------

    def _has_mr_fun(self, fun_name):
        """check if mapper/combiner/reducer is overided by sub-class"""
        for s in ('mapper', 'combiner', 'reducer'):
            if fun_name.startswith(s): break
        else:
            return False

        return bool(getattr(self, fun_name, None))

    def _read_lines(self, protocol):
        for line in self._stdin:
            key, value = protocol.read(line.rstrip(b'\r\n'))
            yield key, value

    def _write_line(self, key, value, protocol):
        self._stdout.write(protocol.write(key, value) + b'\n')
        self._stdout.flush()

    def _run_mapper(self):
        if self._has_mr_fun('mapper_init'):
            logger.info('running mapper_init ...')
            for out_key, out_value in self.mapper_init() or ():
                self._write_line(out_key, out_value, self.internal_protocol)
            logger.info('mapper_init completed')

        logger.info('running mapper ...')
        for key, value in self._read_lines(self.input_protocol):
            for out_key, out_value in self.mapper(key, value) or ():
                self._write_line(out_key, out_value, self.internal_protocol)
        logger.info('mapper completed')

        if self._has_mr_fun('mapper_final'):
            logger.info('running mapper_final ...')
            for out_key, out_value in self.mapper_final() or ():
                self._write_line(out_key, out_value, self.internal_protocol)
            logger.info('mapper_final completed')

    def _run_combiner(self):
        if self._has_mr_fun('combiner_init'):
            logger.info('running combiner_init ...')
            for out_key, out_value in self.combiner_init() or ():
                self._write_line(out_key, out_value, self.internal_protocol)
            logger.info('combiner_init completed')

        logger.info('running combiner ...')
        for key, kv_pairs in itertools.groupby(
                self._read_lines(self.internal_protocol), key=itemgetter(0)):
            values = (v for k, v in kv_pairs)
            for out_key, out_value in self.combiner(key, values) or ():
                self._write_line(out_key, out_value, self.internal_protocol)
        logger.info('combiner completed')

        if self._has_mr_fun('combiner_final'):
            logger.info('running combiner_final ...')
            for out_key, out_value in self.combiner_final() or ():
                self._write_line(out_key, out_value, self.internal_protocol)
            logger.info('combiner_final completed')

    def _run_reducer(self):

        # out_key or out_value might be None and should not be output when being None,
        # so we merge out_key into out_value and use TextValueProtocol,
        # instead of TextProtocol, as the output protocol.
        def combine_key_value(key, value):
            if key is None and value is None:
                raise ValueError('reducer should return `(key, value)` pairs, and at least one is not None.')
            if key is None: return value
            if value is None: return key
            return flatten([key, value])

        if self._has_mr_fun('reducer_init'):
            logger.info('running reducer_init ...')
            for out_key, out_value in self.reducer_init() or ():
                self._write_line(None, combine_key_value(out_key, out_value), self.output_protocol)
            logger.info('reducer_init completed')

        logger.info('running reducer ...')
        for key, kv_pairs in itertools.groupby(
                self._read_lines(self.internal_protocol), key=itemgetter(0)):
            values = (v for k, v in kv_pairs)
            for out_key, out_value in self.reducer(key, values) or ():
                self._write_line(None, combine_key_value(out_key, out_value), self.output_protocol)
        logger.info('reducer completed')

        if self._has_mr_fun('reducer_final'):
            logger.info('running reducer_final ...')
            for out_key, out_value in self.reducer_final() or ():
                self._write_line(None, combine_key_value(out_key, out_value), self.output_protocol)
            logger.info('reducer_final completed')

    @staticmethod
    def is_launched():
        """check if MRJob is initialized and then initialize it."""
        is_launched = os.getenv('HADOOP_IDENT_STRING') or os.getenv('_MRJOB_LAUNCHED')
        return bool(is_launched)

    def _launch(self):
        """change is_launched flag as True"""
        os.environ['_MRJOB_LAUNCHED'] = '1'

    def run(self, runner='hadoop', **kwargs):
        """入口函数，用户执行该方法即可启动作业"""
        runner_class_mapper = {
            'local': LocalRunner,
            'hadoop': HadoopRunner,
        }

        parser = argparse.ArgumentParser()
        parser.add_argument(
            '-r', '--runner', dest='runner', default=None,
            choices=runner_class_mapper.keys(),
            help='where to run the job')
        parser.add_argument(
            '--mapper', dest='run_mapper', default=False, action='store_true',
            help='run mapper')
        parser.add_argument(
            '--combiner', dest='run_combiner', default=False, action='store_true',
            help='run combiner')
        parser.add_argument(
            '--reducer', dest='run_reducer', default=False, action='store_true',
            help='run reducer')

        args, unrecognized = parser.parse_known_args()

        # 中间命令，只需调用相应方法
        if args.run_mapper:
            self._run_mapper()
            return
        if args.run_combiner:
            self._run_combiner()
            return
        if args.run_reducer:
            self._run_reducer()
            return

        # set is_launched flag as True
        self._launch()

        # 启动命令，由相应的 Runner 来执行，命令行中传入的 runner 可覆盖函数参数中传入的 runner。
        # 比如 HadoopRunner 会生成相应的 hadoop streaming 命令行并执行。
        if getattr(args, 'runner', None):
            runner = args.runner

        RunnerClass = runner_class_mapper[runner]

        # 将函数参数与命令行参数同时传给 runner。其中命令行参数会覆盖函数参数
        runner_obj = RunnerClass(self, cmd_args=unrecognized, **kwargs)
        runner_obj.execute()


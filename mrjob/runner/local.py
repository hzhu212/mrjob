# -*- coding: utf-8 -*-

import argparse
import fileinput
import logging
import glob
import os
import subprocess
import sys

from ..util import non_blocking_communicate


logger = logging.getLogger('mrjob')


class LocalRunner(object):
    """local runner for MRJob.
    Run Map-Reduce job on localhost with subprocess. Mainly for testing.
    """

    ALL_OPTS = {'input', 'output', 'mapper', 'combiner', 'reducer'}
    REQUIRED_OPTS = set()
    # local runner should not process data bigger than 50MB or 500000 lines
    MAX_INPUT = 50e6
    MAX_INPUT_LINES = 500000

    def __init__(self, mrjob, cmd_args=None, **kwargs):
        self.mrjob = mrjob

        options = kwargs

        if cmd_args is not None:
            cmd_options = self._parse_cmd_args(cmd_args)

        options.update(cmd_options)
        options = self._validate_options(options)
        self._options = self._default_mr_options()
        self._options.update(options)

    def _parse_cmd_args(self, cmd_args):
        options = {}

        parser = argparse.ArgumentParser()
        parser.add_argument(
            '-input', dest='input', action='append',
            help='Input location for mapper. The same as `hadoop streaming -input`.')
        parser.add_argument(
            '-output', dest='output',
            help='Output location for reducer. The same as `hadoop streaming -output`.')
        args = parser.parse_args(cmd_args)

        for name in ('input', 'output'):
            if getattr(args, name, None):
                options[name] = getattr(args, name)

        return options

    def _validate_options(self, options):
        logger.info('checking job config ...')

        # check required options
        for op in self.REQUIRED_OPTS:
            if op not in options:
                raise ValueError('option "{}" is required'.format(op))

        # warning unknown options
        for op in options:
            if op not in self.ALL_OPTS:
                logger.warning('unknown option "{}={}", ignored.'.format(op, options[op]))

        # check input
        if 'input' in options and options['input'] != ['-']:
            if isinstance(options['input'], basestring):
                options['input'] = [options['input']]
            elif isinstance(options['input'], (list, tuple)):
                if not isinstance(options['input'][0], basestring):
                    raise ValueError('option "input" should be a string or a sequence of strings')

            parsed_input = []
            for path in options['input']:
                parsed = glob.glob(path)
                if not parsed:
                    raise ValueError('input path "{}" not exist'.format(path))
                parsed_input.extend(parsed)

            input_ = []
            for path in set(parsed_input):
                if os.path.isdir(path):
                    logger.warning('"{}" is a directory'.format(path))
                    continue
                input_.append(path)
            options['input'] = input_

            if sum(os.path.getsize(p) for p in options['input']) > self.MAX_INPUT:
                raise ValueError(
                    'LocalRunner is mainly used for testing, but the input '
                    'data is too large(>{}bytes)'.format(self.MAX_INPUT))
        else:
            # default read from stdin
            options['input'] = ['-']

        # check output
        if 'output' in options:
            path = options['output']
            if os.path.isdir(path):
                raise ValueError('option "output"({}) is an existing directory'.format(path))
            if os.path.isfile(path) and os.path.getsize(path) != 0:
                raise ValueError('option "output"({}) is an existing file and not empty'.format(path))
        else:
            # default output to stdout
            options['output'] = '-'

        # check mapper/combiner/reducer
        for name in ('mapper', 'combiner', 'reducer'):
            if name not in options:
                continue
            if not isinstance(options[name], basestring):
                raise ValueError('option "{}" should be a string'.format(name))

        logger.info('job config OK.')
        return options

    def _default_mr_options(self):
        """get default -mapper/-combiner/-reducer options"""
        res = {}
        py_script = sys.argv[0]
        for name in ('mapper', 'combiner', 'reducer'):
            if not self.mrjob._has_mr_fun(name):
                continue
            res[name] = 'python "{}" --{}'.format(py_script, name)
        return res


    def execute(self):
        # if not self.mrjob._has_mr_fun('mapper'):
        #     raise ValueError('You have to implement the "mapper" method')

        def generate_inputs():
            for i, line in enumerate(
                    fileinput.input(files=self._options['input'], mode='rb')):
                if i >= self.MAX_INPUT_LINES:
                    logger.warning(
                        'LocalRunner is mainly used for testing, but the input data '
                        'is too large(>{}lines). Exceeding lines will be ignored'
                        .format(self.MAX_INPUT_LINES))
                    break
                yield line

        # run mapper/combiner/reducer
        inputs = generate_inputs()
        outputs = None
        names = [name for name in ('mapper', 'combiner', 'reducer') if name in self._options]
        for i, name in enumerate(names):
            logger.debug('running {} ...'.format(name))
            cmd = self._options[name]
            proc = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, shell=True)
            out = non_blocking_communicate(proc, inputs)
            if i < len(names) - 1:
                # sort out lines
                inputs = sorted(out, key=lambda line: line.split(b'\t', 1)[0])
            else:
                outputs = out

        # write last_out to output
        if self._options['output'] == '-':
            fout = sys.stdout
        else:
            fout = open(self._options['output'], 'wb')

        for line in outputs:
            fout.write(line)

        if self._options['output'] != '-':
            fout.close()

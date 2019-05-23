# -*- coding: utf-8 -*-

from collections import Iterable
import errno
import os
import re
import select
from threading import Thread


class TimeoutError(IOError): pass


def non_blocking_communicate(proc, inputs):
    """non blocking version of subprocess.Popen.communicate.
    `inputs` should be a sequence of bytes (e.g. file-like object, generator,
    io.BytesIO, etc.).
    """

    def write_proc(proc, inputs):
        for line in inputs:
            try:
                proc.stdin.write(line)
            except IOError as e:
                # break at "Broken pipe" error, or "Invalid argument" error.
                if e.errno == errno.EPIPE or e.errno == errno.EINVAL:
                    break
                else:
                    raise
        proc.stdin.close()

    t = Thread(target=write_proc, args=(proc, inputs))
    t.start()

    while proc.poll() is None:
        if select.select([proc.stdout], [], [])[0]:
            line = proc.stdout.readline()
            if not line:
                break
            yield line

    proc.wait()
    t.join()


def non_breaking_communicate(proc, input, timeout=None, multiple_output=False):
    """Communicate multiple times with a process without breaking the pipe."""

    if not isinstance(input, str):
        raise ValueError('input should be a str')
    if not input.endswith(b'\n'):
        input += b'\n'

    proc.stdin.write(input)
    proc.stdin.flush()

    if select.select([proc.stdout], [], [], timeout)[0]:
        res = proc.stdout.readline()
    else:
        raise TimeoutError('process failed giving any response in {} seconds'.format(timeout))

    # there might be multiple output lines
    if multiple_output:
        res = [res]
        while select.select([proc.stdout], [], [], timeout)[0]:
            out = proc.stdout.readline()
            res.append(out)

    return res


def flatten(lst):
    """flatten irregular nested list.
    see: https://stackoverflow.com/questions/2158395/flatten-an-irregular-list-of-lists
    """
    for el in lst:
        if isinstance(el, Iterable) and not isinstance(el, basestring):
            for sub in flatten(el):
                yield sub
        else:
            yield el

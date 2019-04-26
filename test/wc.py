# -*- coding: utf-8 -*-

import logging
import os
import sys

from mrjob import MRJob


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger('wc')

cur_dir = os.path.dirname(os.path.abspath(__file__))


class WordCount(MRJob):
    """A simple word-count job"""

    def mapper(self, _, line):
        yield 'line', 1
        yield 'word', len(line.strip().split())
        yield 'char', len(line)

    def reducer(self, key, values):
        yield key, sum(values)

    combiner = reducer


if __name__ == '__main__':
    job = WordCount()

    if job.is_launched():
        job.run()
        exit()

    # job.run(runner='local', input=os.path.join(cur_dir, 'data.txt'))

    job.run(
        input='afs://tianqi.afs.baidu.com:9902/user/ubs/pv/common/feed_os_version.txt',
        output='afs://tianqi.afs.baidu.com:9902/user/ubs/pv/zhuhe02/tmp/test_mrjob/',
        merge_output=1,
        jobconf={
            'mapred.job.name': 'zhuhe02_word_count_by_mrjob',
            'mapred.reduce.tasks': 1,
            'mapred.job.queue.name': 'ubs-pv-chunjie',
        })

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


    # call LocalRunner like this:
    # job.run(runner='local', input=os.path.join(cur_dir, 'data.txt'))


    # reset hadoop python archive like this:
    from mrjob.runner.hadoop import set_hadoop_python
    set_hadoop_python(
        '/user/ubs/pv/common/python272.tar.gz#python2.7.2',
        'python2.7.2/python2.7/bin/python')


    # call HadoopRunner like this
    job.run(
        # you can reset hadoop client with `hadoop` argument
        hadoop='/home/work/hadoop-client-yq/hadoop/bin/hadoop',

        # other arguments is the same like `hadoop streaming`
        input='afs://tianqi.afs.baidu.com:9902/user/ubs/pv/common/feed_os_version.txt',
        output='afs://tianqi.afs.baidu.com:9902/user/ubs/pv/zhuhe02/tmp/test_mrjob/',

        # `jobconf` argument is the same like `hadoop streaming -jobconf` or `hadoop streaming -D`
        jobconf={
            'mapred.job.name': 'zhuhe02_word_count_by_mrjob',
            'mapred.reduce.tasks': 1,
            'mapred.job.queue.name': 'ubs-pv-chunjie',
        })

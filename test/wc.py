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
        logger.info('I am in mapper now')
        yield 'line', 1
        yield 'word', len(line.strip().split())
        yield 'char', len(line)

    def combiner_init(self):
        yield 'combiner_init', 1

    def combiner(self, key, values):
        logger.info('I am in combiner now')
        yield key, sum(values)

    def combiner_final(self):
        yield 'combiner_final', 1

    def reducer(self, key, values):
        logger.info('I am in reducer now')
        yield key, sum(values)


if __name__ == '__main__':
    job = WordCount()

    if job.is_launched():
        job.run()
        exit()


    # # call LocalRunner like this:
    # job.run(runner='local', input=os.path.join(cur_dir, 'data.txt'))
    # exit()


    # # reset hadoop python archive like this:
    # from mrjob.runner.hadoop import set_hadoop_python
    # set_hadoop_python(
    #     'hdfs://localhost:9902/user/zhuhe212/another/python272.tar.gz#python2.7.2',
    #     'python2.7.2/python2.7/bin/python')


    # call HadoopRunner like this
    job.run(
        # # you can reset hadoop client with `hadoop` argument
        # hadoop='/home/zhuhe212/another/hadoop/bin/hadoop',

        # other arguments is the same like `hadoop streaming`
        input='hdfs://localhost:9902/user/zhuhe212/tmp/feed_os_version.txt',
        output='hdfs://localhost:9902/user/zhuhe212/tmp/test_mrjob/',
        merge_output=1,
        # `jobconf` argument is the same like `hadoop streaming -jobconf` or `hadoop streaming -D`
        jobconf={
            'mapred.job.name': 'zhuhe212_word_count_by_mrjob',
            'mapred.reduce.tasks': 2,
            'dce.shuffle.enable': 'false',
        })

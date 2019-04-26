# -*- coding: utf-8 -*-

import logging
import os
import re
import sys

from mrjob import MRJob


logging.basicConfig(level=logging.DEBUG)

cur_dir = os.path.dirname(os.path.abspath(__file__))
BE_SET = {'am', 'is', 'are', 'was', 'were', 'be', 'being', 'been'}


class WordCountPro(MRJob):
    """统计指定的词的出现次数，并将所有的 be 动词汇总为一个词"""

    def mapper_init(self):
        # 载入限定的词汇表
        self.word_set = set()
        with open(os.path.join(cur_dir, 'word_list.txt'), 'r') as f:
            for line in f:
                self.word_set.add(line.strip())
        self.word_set.update(BE_SET)

    def mapper(self, _, line):
        for word in re.findall(r'\w+', line.strip()):
            # 只统计词汇表中的单词
            if word in self.word_set:
                yield word, 1


    def reducer_init(self):
        # 初始化 be 动词计数
        self.be_count = 0

    def reducer(self, key, values):
        if key not in BE_SET:
            yield key, sum(values)
        else:
            # be 动词单独统计
            self.be_count += sum(values)

    def reducer_final(self):
        # 将 be 动词作为一个条目输出
        yield 'verb_be', self.be_count


if __name__ == '__main__':
    job = WordCountPro()

    if job.is_launched():
        job.run()
        exit()

    job.run(runner='local', input=os.path.join(cur_dir, 'data.txt'))

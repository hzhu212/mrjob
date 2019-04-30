# -*- coding: utf-8 -*-

"""
@Author:      zhuhe02
@Email:       zhuhe02@baidu.com
@CreateAt:    2019-04-16
@Description:
    A Map-Reduce framework for Python.

    The API design is learnt from mrjob(https://pythonhosted.org/mrjob/index.html).

    This framework makes it very easy to create a map-reduce job: just write
    mapper/combiner/reducer functions which yields key-value pairs.
"""


from job import MRJob
from runner.hadoop import bundle, set_hadoop_python


__title__ = 'mrjob'
__description__ = 'A Map-Reduce framework for Python.'
__version__ = '1.0.0'
__author__ = 'zhuhe02'
__author_email__ = 'zhuhe02@baidu.com'


__all__ = ['__version__', '__author__', 'MRJob']

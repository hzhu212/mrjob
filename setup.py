# -*- coding: utf-8 -*-

from distutils.core import setup

import mrjob as about

setup(
    name=about.__title__,
    version=about.__version__,
    description=about.__description__,
    author=about.__author__,
    author_email=about.__author_email__,
    packages=['mrjob'],
)

import os
os.environ['RIVERSNAKE_ZOOKEEPER_HOSTS'] = '10.141.141.10:2181'

import zoodict

__author__ = 'jleslie'

import unittest
from hamcrest import *

from task_chain import TaskChain

class Test(unittest.TestCase):

    def test_Can_join_one_task_to_another(self):
        chain = TaskChain("unittest", _zoodict_impl_for_testing=zoodict.MockZooDictForTesting)

        task1 = chain.register_task('tests.tasks.Task1')
        task2 = chain.register_task('tests.tasks.Task2')

        chain.join_tasks(task1, task2, status_code='example')

        task_id = chain.find_output_task(task1, 'example')
        assert_that(task_id, equal_to(task2))
import os
os.environ['RIVERSNAKE_ZOOKEEPER_HOSTS'] = '10.141.141.10:2181'

import zoodict

__author__ = 'jleslie'

import unittest
from hamcrest import *

from task_chain import DeployedTaskChain

class Test(unittest.TestCase):

    def test_Can_join_one_task_to_another(self):
        chain = TaskChain("unittest", _zoodict_impl_for_testing=zoodict.MockZooDictForTesting)

        chain.join_tasks('tests.tasks.Task1', 'tests.tasks.Task2', status_code='example')

        task_name = chain.find_output_task('tests.tasks.Task1', 'example')
        assert_that(task_name, equal_to('tests.tasks.Task2'))
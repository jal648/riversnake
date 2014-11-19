import time

__author__ = 'jleslie'


import os
os.environ['RIVERSNAKE_ZOOKEEPER_HOSTS'] = '10.141.141.10:2181'

import unittest
from hamcrest import *

from riversnake import Task
from riversnake.task_chain import TaskChain, DeployedTaskChain
from riversnake.deploy import MarathonDeployer

from riversnake.tests.example_tasks import taskname, AddThree, DifferentStatusIfGreatherThanFive, RepeatTenTimes, SumAmountInternal, WriteToALocalFile

class Test(unittest.TestCase):

    def setUp(self):
        task_chain = TaskChain('test')

        task_chain.clear_all_tasks()

        # add_three = task_chain.register_task(taskname(AddThree))
        # self.add_three = add_three
        #
        # is_above_or_below_five = task_chain.register_task(taskname(DifferentStatusIfGreatherThanFive))
        # repeat_ten = task_chain.register_task(taskname(RepeatTenTimes))
        # sum_it = task_chain.register_task(taskname(SumAmountInternal))
        #
        # task_chain.join_tasks(add_three, is_above_or_below_five)
        # task_chain.join_tasks(is_above_or_below_five, sum_it, 'high')
        # task_chain.join_tasks(is_above_or_below_five, repeat_ten, 'low')
        # task_chain.join_tasks(repeat_ten, sum_it)
        #
        # self.task_chain = task_chain
        #
        # self.deployed_chain = DeployedTaskChain(task_chain, 'test')
        #
        # self.invokers = []
        # for task_id in [ add_three, is_above_or_below_five, repeat_ten, sum_it ]:
        #     # clear the queues before running the test (in casea  previous run failed and stuff is hanging around.
        #     self.deployed_chain.get_input_queue(task_id)._declare_queue().__enter__().queue_purge()


    def test_A_chain_of_tasks(self):

        task_chain = TaskChain('test')

        task_chain.clear_all_tasks()
        task_id = task_chain.register_task(taskname(WriteToALocalFile))

        deployed_chain = DeployedTaskChain(task_chain, 'mtest')


        m = MarathonDeployer('http://10.141.141.10:8080')

        m.deploy(task_chain, 'mtest')

        q = deployed_chain.get_input_queue(task_id)

        for i in xrange(1000):
            q.write({'test':'Hello World!'})
            time.sleep(2)

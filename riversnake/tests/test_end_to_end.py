import os
from riversnake import TaskInvoker, Task

os.environ['RIVERSNAKE_ZOOKEEPER_HOSTS'] = '10.141.141.10:2181'

import unittest

from hamcrest import *
from task_chain import TaskChain, DeployedTaskChain

__author__ = 'jleslie'

class AddThree(Task):
    def run(self, amount=0):
        return dict(amount=amount+3)

class DifferentStatusIfGreatherThanFive(Task):
    def run(self, amount=0):
        return dict(amount=amount), 'low' if amount <= 5 else 'high'

class RepeatTenTimes(Task):
    def run(self, **kwargs):
        return [ kwargs for i in range(10) ]



class SumAmountInternal(Task):

    value=0

    def run(self, **kwargs):
        self.value += kwargs['amount']

def taskname(task):
    return 'tests.test_end_to_end.' + task.__name__

class Test(unittest.TestCase):

    def setUp(self):
        task_chain = TaskChain('test')


        task_chain.join_tasks(
                    taskname(AddThree),
                    taskname(DifferentStatusIfGreatherThanFive))

        task_chain.join_tasks(
                    taskname(DifferentStatusIfGreatherThanFive),
                    taskname(RepeatTenTimes),
                    'low')

        task_chain.join_tasks(
                    taskname(DifferentStatusIfGreatherThanFive),
                    taskname(SumAmountInternal),
                    'high')

        task_chain.join_tasks(
                    taskname(RepeatTenTimes),
                    taskname(SumAmountInternal))

        self.deployed_chain = DeployedTaskChain(task_chain, 'test')

        self.invokers = []
        for task in [ AddThree, DifferentStatusIfGreatherThanFive, RepeatTenTimes, SumAmountInternal ]:
            self.deployed_chain.get_input_queue(taskname(task))._declare_queue().clear()

            self.invokers.append( TaskInvoker(taskname(task), self.deployed_chain) )

        self.summed_amount_task = self.invokers[-1].task

    def test_A_chain_of_tasks(self):

        self.deployed_chain.get_input_queue(taskname(AddThree)).write_data(amount=20)

        for i in range(2):
            for invoker in self.invokers:
                invoker.poll(wait_time_seconds=1)

        assert_that(self.summed_amount_task.value, equal_to(20+3))


    def test_Chains_low_path(self):

        self.deployed_chain.get_input_queue(taskname(AddThree)).write_data(amount=1)

        for i in range(12):
            print "Summed:", self.summed_amount_task.value

            for invoker in self.invokers:
                invoker.poll(wait_time_seconds=1)


        assert_that(self.summed_amount_task.value, equal_to(1+3*10))

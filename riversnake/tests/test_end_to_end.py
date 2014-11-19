import os
from riversnake import TaskInvoker, Task

os.environ['RIVERSNAKE_ZOOKEEPER_HOSTS'] = '10.141.141.10:2181'

import unittest

from hamcrest import *
from riversnake.task_chain import TaskChain, DeployedTaskChain

from riversnake.tests.example_tasks import taskname, AddThree, DifferentStatusIfGreatherThanFive, RepeatTenTimes, SumAmountInternal

class Test(unittest.TestCase):

    def setUp(self):
        task_chain = TaskChain('test')

        add_three = task_chain.register_task(taskname(AddThree))
        self.add_three = add_three

        is_above_or_below_five = task_chain.register_task(taskname(DifferentStatusIfGreatherThanFive))
        repeat_ten = task_chain.register_task(taskname(RepeatTenTimes))
        sum_it = task_chain.register_task(taskname(SumAmountInternal))

        task_chain.join_tasks(add_three, is_above_or_below_five)
        task_chain.join_tasks(is_above_or_below_five, sum_it, 'high')
        task_chain.join_tasks(is_above_or_below_five, repeat_ten, 'low')
        task_chain.join_tasks(repeat_ten, sum_it)

        self.deployed_chain = DeployedTaskChain(task_chain, 'test')

        self.invokers = []
        for task_id in [ add_three, is_above_or_below_five, repeat_ten, sum_it ]:
            # clear the queues before running the test (in casea  previous run failed and stuff is hanging around.
            self.deployed_chain.get_input_queue(task_id)._declare_queue().__enter__().queue_purge()
            self.invokers.append( TaskInvoker(task_id, self.deployed_chain) )


        self.summed_amount_task = self.invokers[-1].task

    def test_A_chain_of_tasks(self):

        self.deployed_chain.get_input_queue(self.add_three).write_data(amount=20)

        for i in range(2):
            for invoker in self.invokers:
                invoker.poll(wait_time_seconds=1)

        assert_that(self.summed_amount_task.value, equal_to(20+3))


    def test_Chains_low_path(self):

        self.deployed_chain.get_input_queue(self.add_three).write_data(amount=1)

        for i in range(12):
            print "Summed:", self.summed_amount_task.value

            for invoker in self.invokers:
                invoker.poll(wait_time_seconds=1)


        assert_that(self.summed_amount_task.value, equal_to( (1+3)*10 ))

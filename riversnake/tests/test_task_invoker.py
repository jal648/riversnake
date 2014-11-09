from riversnake import TaskInvoker, Task
from task_chain import DeployedTaskChain, TaskChain
import work_queue
import zoodict

__author__ = 'jleslie'

import unittest
from hamcrest import *

class TestTask1(Task):

    def run(self, **kwargs):
        d = dict()
        d.update(kwargs)
        d['this_happened'] = True
        return d

class TestTaskMultipleResponse(Task):

    def run(self, **kwargs):

        return [
              ( dict(foo=1), 'success' ),
              ( dict(foo=2), 'success' ),
              ( dict(foo=3), 'other_status' )
            ]

class Test(unittest.TestCase):

    def test_Reads_from_input_queue_and_runs_task_when_message_is_found(self):

        task_chain = TaskChain('test', _zoodict_impl_for_testing=zoodict.MockZooDictForTesting)



        deployed_chain = DeployedTaskChain(task_chain, 'test', _work_queue_impl_for_testing=work_queue.lookup_test_work_queue)

        invoker = TaskInvoker('tests.test_task_invoker.TestTask1', deployed_chain)

        task_chain.join_tasks('tests.test_task_invoker.TestTask1', 'tests.test_task_invoker.TestTask2')

        deployed_chain.get_input_queue('tests.test_task_invoker.TestTask1').write(dict(a=1, b=2))

        invoker.poll()

        put_on_queue = deployed_chain.get_input_queue('tests.test_task_invoker.TestTask2').read()
        assert_that(put_on_queue, equal_to(dict(a=1, b=2, this_happened=True)))

    def test_Tasks_can_return_multiple_messages_and_statuses_to_be_routed_to_different_queues(self):
        task_chain = TaskChain('test', _zoodict_impl_for_testing=zoodict.MockZooDictForTesting)
        deployed_chain = DeployedTaskChain(task_chain, 'test', _work_queue_impl_for_testing=work_queue.lookup_test_work_queue)

        invoker = TaskInvoker('tests.test_task_invoker.TestTaskMultipleResponse', deployed_chain)
        task_chain.join_tasks('tests.test_task_invoker.TestTaskMultipleResponse', 'tests.test_task_invoker.TestTask2', 'success')
        task_chain.join_tasks('tests.test_task_invoker.TestTaskMultipleResponse', 'tests.test_task_invoker.TestTask3', 'other_status')

        deployed_chain.get_input_queue('tests.test_task_invoker.TestTaskMultipleResponse').write(dict(x=1))

        invoker.poll()

        first_result = deployed_chain.get_input_queue('tests.test_task_invoker.TestTask2').read()
        second_result = deployed_chain.get_input_queue('tests.test_task_invoker.TestTask2').read()
        third_result = deployed_chain.get_input_queue('tests.test_task_invoker.TestTask2').read()

        assert_that(first_result, equal_to(dict(foo=1)))
        assert_that(second_result, equal_to(dict(foo=2)))
        assert_that(third_result, is_(none()), "The third result should be routed to a different queue, this one should be empty")

        other_status_result = deployed_chain.get_input_queue('tests.test_task_invoker.TestTask3').read()
        assert_that(other_status_result, equal_to(dict(foo=3)))

    def test_Tasks_can_return_files_as_part_of_messages_which_are_transparently_stored_in_HFDS(self):
        pass



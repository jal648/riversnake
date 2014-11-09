from hamcrest import *
import unittest
import uuid
import work_queue

__author__ = 'jleslie'

class Test(unittest.TestCase):

    def test_Will_create_queue_automatically(self):

        new_q = work_queue.WorkQueueOnAmazonSQS('test'+str(uuid.uuid1()))

        res = new_q.read(1)

        assert_that(res, is_(none()))

        new_q.write(dict(foo='bar'))

        assert_that(new_q.read(), equal_to(dict(foo='bar')))
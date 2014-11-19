from operator import itemgetter

__author__ = 'jleslie'
import os
os.environ['RIVERSNAKE_ZOOKEEPER_HOSTS'] = '10.141.141.10:2181'

import unittest
from hamcrest import *

from riversnake.zoodict import ZooDict
import riversnake.zoodict as zoodict

class Test(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # ensure a zookper instance is up for testing

        try:
            zoodict.ZooDict().__enter__()
        except Exception as e:
            raise RuntimeError("ZooDict tests require an instance of zookeeper running, currently we're looking for one here: {}\n\nException: {}".format(zoodict.zk_hosts, e))


    def test_Can_store_and_retrieve_a_python_dict(self):
        with ZooDict() as zd:
            stored_data = dict(a=1, b=2)
            zd.set('key1', stored_data)
            retrieved = zd.get('key1')
            assert_that(retrieved, equal_to(stored_data))

    def test_Fails_ungracefully_if_dict_contains_unjsonable_things(self):
        import datetime

        with ZooDict() as zd:
            stored_data = dict(will_fail=datetime.date.today(), b=2)
            try:
                zd.set('key2', stored_data)
            except Exception as e:
                # yep, just an exception
                pass
            else:
                raise AssertionError("We expected an exception to be raised when json'ifying a date, but it wasn't")


    def test_Returns_none_if_key_not_found(self):
        with ZooDict() as zd:
            import uuid
            should_not_exist = zd.get(str(uuid.uuid1()))
            assert_that(should_not_exist, is_(none()))

    def test_can_specify_a_different_path_to_store_items(self):
        with ZooDict() as zd:
            stored_data = dict(a=1, b=2)
            zd.set('key1', stored_data)
            retrieved = zd.get('key1')
            assert_that(retrieved, equal_to(stored_data))



    def test_Can_iterate_item_tuples_similar_to_a_dict(self):

        with ZooDict() as zd:
            stored_data = dict(a=1, b=2)
            zd.set('key1', dict(a=1))
            zd.set('key2', dict(a=2))
            zd.set('key3', dict(a=3))

            all_items = zd.items()
            assert_that(all_items, has_length(3), "Should be three stored values")

            all_keys = map(itemgetter(0), all_items)
            assert_that(all_keys, contains_inanyorder('key1', 'key2', 'key3'), "Should contain all keys")

            all_values  = map(itemgetter(1), all_items)
            assert_that(all_values, contains_inanyorder(
                has_entries(a=1),
                has_entries(a=2),
                has_entries(a=3),
            ))

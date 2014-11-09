import json
from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError

__author__ = 'jleslie'

"""A simple library for storing keys -> json dicts in zookeeper. Not concerned about collisions are race conditions, just need
the distributed data store"""

import kazoo
import os

zk_hosts = os.environ.get('RIVERSNAKE_ZOOKEEPER_HOSTS') or "127.0.0.1:2181"
default_path_root = os.environ.get('RIVERSNAKE_ZOODICT_ROOT') or "/zoodict"

class ZooDict(object):

    def __init__(self, path_root=None):
        self.path_root = path_root or default_path_root

    def __enter__(self):
        self.zk = KazooClient(hosts=zk_hosts)
        self.zk.start(timeout=5)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.zk.stop()

    def start(self):
        return self.__enter__()

    def stop(self):
        return self.__exit__()

    def get(self, key):
        try:
            r = self.zk.get(self.path_root + '/' + key)

            if r and r[0]:
                return json.loads(r[0])
        except NoNodeError as no_node:
            return None

    def set(self, key, data_dict):
        self.zk.ensure_path(self.path_root + '/' + key)
        self.zk.set(self.path_root + '/' + key, json.dumps(data_dict))



class MockZooDictForTesting(ZooDict):

    just_a_dict = {}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def get(self, key):
        return self.just_a_dict.get(key)

    def set(self, key, data_dict):
        self.just_a_dict[key] = data_dict


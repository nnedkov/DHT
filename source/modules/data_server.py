#!/usr/bin/python

import logging
from collections import defaultdict
import config
from datetime import datetime, timedelta

logging.basicConfig(level=config.LOG_LEVEL,
                    format='%(name)s: %(message)s', )

logger = logging.getLogger('DataServer')

MAX_DURATION = 43200  # seconds


class DataServer:
    def __init__(self):
        self.storage = defaultdict(list)

    def add(self, key, data, ttl=MAX_DURATION):
        try:
            if key in self.storage:
                for index, item in enumerate(self.storage[key]):
                    if item.data == data:
                        self.storage[key][index].time = datetime.now()
                        self.storage[key][index].ttl = ttl
                        return 0
            item = DataItem(data, datetime.now(), ttl)
            self.storage[key].append(item)
            return 0
        except:
            return -2


    def remove(self, key, data=None):
        if key in self.storage:
            if data is None:
                del self.storage[key]
                return True
            else:
                for item in self.storage[key]:
                    if item.data == data:
                        self.storage[key].remove(item)
                        return True
        return False

    def get(self, key):
        if key in self.storage:
            items = []
            for item in self.storage[key]:
                expiry = item.time + timedelta(seconds=item.ttl)
                if expiry > datetime.now():
                    items.append(item.data)
                else:
                    self.storage[key].remove(item)
            if not items:
                return None
            else:
                return items

    def validate(self):
        for key in self.storage:
            for item in self.storage[key][:]:
                expiry = item.time + timedelta(seconds=item.ttl)
                if expiry < datetime.now():
                    self.storage[key].remove(item)


class DataItem:
    def __init__(self, data, time, ttl):
        self.data = data
        self.time = time
        self.ttl = ttl


if __name__ == '__main__':
    data = DataServer()
    print data.storage
    item1 = "bla"
    item2 = "bla "
    data.add("12", item1)
    data.add("12", item1)
    data.add("13", item1, 50)
    data.add("12", item2, 50)
    data.add("13", item2, 50)
    for i in data.storage:
        for j in range(len(data.storage[i])):
            data.storage[i][j].time = datetime.now() - timedelta(seconds=10)
    data.validate()
    print data.storage

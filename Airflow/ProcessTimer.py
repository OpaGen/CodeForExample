# coding: utf-8
# Python version 3.7

from time import time
import datetime

class ProcessTimer:
    def __init__ (self):
        self.ts_start = None
        self.ts_finish = None

    def start (self):
        self.ts_start = time()

    def finish (self):
        self.ts_finish = time()

    def getActiveTime (self):
        if self.ts_start is None or self.ts_finish is None:
            return None

        return self.ts_finish - self.ts_start

    def getStart (self):
        return self.ts_start

    def getFinish (self):
        return self.ts_finish
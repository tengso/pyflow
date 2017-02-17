import unittest
import datetime

from flow.core import RealTimeEngine, RealTimeDataSource, Flow, Input, when


class TestRealTime(unittest.TestCase):
    def testSimple(self):
        engine = RealTimeEngine(keep_history=True)

        now = datetime.datetime.now()
        d1 = RealTimeDataSource('test', engine, [
            (now + datetime.timedelta(seconds=1), 1),
            (now + datetime.timedelta(seconds=2), 1),
        ])

        d2 = RealTimeDataSource('test2', engine, [
            (now + datetime.timedelta(seconds=3), 2),
            (now + datetime.timedelta(seconds=4), 2),
        ])

        class Add(Flow):
            input1 = Input()
            input2 = Input()

            def __init__(self, input1, input2):
                super().__init__('x')

            @when(input1, input2)
            def handle(self):
                if self.input1 and self.input2:
                    self << self.input1() + self.input2()

        a = Add(d1, d2)

        start = now
        end = now + datetime.timedelta(seconds=5)
        engine.start(start, end)

        self.assertEqual(len(a()), 2)
        for i, (t, v) in enumerate(a()):
            self.assertEqual(v, 3)

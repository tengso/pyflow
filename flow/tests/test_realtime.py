import unittest
import datetime

from flow.core import RealTimeEngine, RealTimeDataSource, Flow, Input, when, Timer, SampleMethod

import logging

logging.basicConfig(level=logging.DEBUG)


class TestRealTime(unittest.TestCase):
    def testSimple(self):
        engine = RealTimeEngine(keep_history=True)
        now = datetime.datetime.now()

        d1 = [
            (now + datetime.timedelta(seconds=1), 1),
            (now + datetime.timedelta(seconds=2), 1),
        ]

        d2 = [
            (now + datetime.timedelta(seconds=3), 2),
            (now + datetime.timedelta(seconds=4), 3),
        ]

        ds1 = RealTimeDataSource('test', engine, d1)
        ds2 = RealTimeDataSource('test2', engine, d2)

        class Add(Flow):
            input1 = Input()
            input2 = Input()

            def __init__(self, input1, input2):
                super().__init__('x')

            @when(input1, input2)
            def handle(self):
                if self.input1 and self.input2:
                    self << self.input1() + self.input2()

        class Checker(Flow):
            input = Input()

            def __init__(self, input, start_time):
                super().__init__('checker')
                self.start_time = start_time

            @when(input)
            def handel(self2):
                logic = (self2.now() - datetime.datetime.utcfromtimestamp(0)).total_seconds()
                physical = (datetime.datetime.now() - datetime.datetime.utcfromtimestamp(0)).total_seconds()
                self.assertAlmostEqual(abs(logic - physical), 0, places=1)

                if self2.input() == 3:
                    self.assertEqual(self2.now(), d2[0][0])
                elif self2.input() == 4:
                    self.assertEqual(self2.now(), d2[1][0])
                else:
                    raise ValueError('input must be 3 or 4')

                self2 << self2.input()

        start = now
        end = now + datetime.timedelta(seconds=5)

        a = Checker(Add(ds1, ds2), start)

        s = datetime.datetime.now()
        engine.start(start, end)
        e = datetime.datetime.now()
        self.assertAlmostEqual((e - s).total_seconds(), 5, places=1)
        self.assertEqual(len(a()), 2)
        for i, (t, v) in enumerate(a()):
            self.assertEqual(v, d1[i][1] + d2[i][1])
            self.assertEqual(t, d2[i][0])

    def testTimer(self):
        engine = RealTimeEngine(keep_history=True)

        start_time = datetime.datetime.now()
        end_time = start_time + datetime.timedelta(seconds=5)

        input1_time = start_time + datetime.timedelta(seconds=2)
        input2_time = start_time + datetime.timedelta(seconds=3)
        input1_value = 1
        input2_value = 2
        input1 = [(input1_time, input1_value)]
        input2 = [(input2_time, input2_value)]
        default_value = 3

        input1 = RealTimeDataSource('input1', engine, input1)
        input2 = RealTimeDataSource('input2', engine, input2)

        class Transform(Flow):
            input1 = Input()
            input2 = Input()

            timer = Timer()

            def __init__(self, input1, input2, default_value):
                super().__init__("transform")
                self.default_value = default_value
                self.done = False

            @when(input1)
            def handle(self):
                self.timer = self.now() + datetime.timedelta(seconds=0.5)
                print('input1', self.input1())

            @when(input2)
            def handle(self):
                if not self.done:
                    self << self.input1() + self.input2()
                else:
                    self << self.input2()
                print('input2', self.input2())

            @when(timer)
            def do_timer(self):
                if not self.input2 and not self.done:
                    self << self.input1() + self.default_value
                    self.done = True

        t = Transform(input1, input2, default_value)
        engine.start(start_time, end_time)
        result = t()
        print(t())
        # engine.show_graph('timer')
        t, value = result[0]
        self.assertEqual(t, input1_time + datetime.timedelta(seconds=0.5))
        self.assertEqual(value, input1_value + default_value)
        t, value = result[1]
        self.assertEqual(t, input2_time)
        self.assertEqual(value, input2_value)

    def testSample(self):
        engine = RealTimeEngine(keep_history=True)

        start_time = datetime.datetime.now()
        end_time = start_time + datetime.timedelta(seconds=10)

        data = [
            (start_time + datetime.timedelta(seconds=1), 1),
            (start_time + datetime.timedelta(seconds=2), 2),
            (start_time + datetime.timedelta(seconds=3), 3),
            (start_time + datetime.timedelta(seconds=4), 4),
            (start_time + datetime.timedelta(seconds=5), 5),
            (start_time + datetime.timedelta(seconds=6), 6),
            (start_time + datetime.timedelta(seconds=7), 7),
            (start_time + datetime.timedelta(seconds=8), 8),
        ]

        data = RealTimeDataSource('data', engine, data)

        s = data.sample(datetime.timedelta(seconds=2), SampleMethod.First)

        engine.start(start_time, end_time)
        result = s()
        print(result)

        # engine.show_graph('timer')

        t, value = result[0]
        # self.assertEqual(t, input1_time + datetime.timedelta(seconds=0.5))
        self.assertEqual(value, 1)
        t, value = result[1]
        # self.assertEqual(t, input2_time)
        self.assertEqual(value, 3)
        t, value = result[2]
        # self.assertEqual(t, input2_time)
        self.assertEqual(value, 5)
        t, value = result[3]
        # self.assertEqual(t, input2_time)
        self.assertEqual(value, 7)

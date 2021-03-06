import unittest
import datetime
import time

from flow.core import RealTimeEngine, RealTimeDataSource, Flow, Input, when, Timer, SampleMethod, Feedback, Output

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

        raw_data = [
            (start_time + datetime.timedelta(seconds=1), 1),
            (start_time + datetime.timedelta(seconds=2), 2),
            (start_time + datetime.timedelta(seconds=3), 3),
            (start_time + datetime.timedelta(seconds=4), 4),
            (start_time + datetime.timedelta(seconds=5), 5),
            (start_time + datetime.timedelta(seconds=6), 6),
            (start_time + datetime.timedelta(seconds=7), 7),
            (start_time + datetime.timedelta(seconds=8), 8),
        ]

        data = RealTimeDataSource('data', engine, raw_data)

        s = data.sample(datetime.timedelta(seconds=2), SampleMethod.First)

        engine.start(start_time, end_time)
        result = s()
        # print(raw_data)
        # print(result)

        # engine.show_graph('timer')

        t, value = result[0]
        # self.assertEqual(t, input1_time + datetime.timedelta(seconds=0.5))
        self.assertEqual(value, 1)

        t, value = result[1]
        # self.assertEqual(t, input2_time)
        self.assertEqual(value, 4)

        t, value = result[2]
        # self.assertEqual(t, input2_time)
        self.assertEqual(value, 6)

        t, value = result[3]
        # self.assertEqual(t, input2_time)
        self.assertEqual(value, 8)

    def testSampleWithStartWith(self):
        engine = RealTimeEngine(keep_history=True)

        start_time = datetime.datetime.now()
        end_time = start_time + datetime.timedelta(seconds=10)

        raw_data = [
            (start_time + datetime.timedelta(seconds=1), 1),
            (start_time + datetime.timedelta(seconds=2), 2),
            (start_time + datetime.timedelta(seconds=3), 3),
            (start_time + datetime.timedelta(seconds=4), 4),
            (start_time + datetime.timedelta(seconds=5), 5),
            (start_time + datetime.timedelta(seconds=6), 6),
            (start_time + datetime.timedelta(seconds=7), 7),
            (start_time + datetime.timedelta(seconds=8), 8),
        ]

        data = RealTimeDataSource('data', engine, raw_data)

        s = data.sample(datetime.timedelta(seconds=2), SampleMethod.Last, raw_data[3][0])

        engine.start(start_time, end_time)
        result = s()
        # print(raw_data)
        # print(result)

        # engine.show_graph('timer')

        t, value = result[0]
        # self.assertEqual(t, input1_time + datetime.timedelta(seconds=0.5))
        self.assertEqual(value, 4)

        t, value = result[1]
        # self.assertEqual(t, input2_time)
        self.assertEqual(value, 6)

        t, value = result[2]
        # self.assertEqual(t, input2_time)
        self.assertEqual(value, 8)

    def testFeedback(self):

        class N(Flow):
            input1 = Input()
            input2 = Input()

            def __init__(self, input1, input2):
                super().__init__()

            @when(input1, input2)
            def h(self):
                time.sleep(1)
                if self.input1:
                    self << self.input1() + 1
                else:
                    self << self.input2()

        engine = RealTimeEngine()

        n_1 = Feedback(engine)
        start_time = datetime.datetime.now()
        end_time = start_time + datetime.timedelta(seconds=5)
        n = N(n_1, RealTimeDataSource('', engine, [(start_time + datetime.timedelta(seconds=0.5), 1)]))

        n_1 << n

        # engine.show_graph(show_cycle=True)
        engine.start(start_time, end_time)

        self.assertEqual(n()[0][1], 5)

    def testFeedback2(self):

        class PlaceOrder(Flow):
            error_message = Input()
            action = Input()

            def __init__(self, error_message, action):
                super().__init__()
                self.counter = 0

            @when(error_message)
            def h(self):
                self << self.error_message()

            @when(action)
            def h(self):
                self << self.counter
                self.counter += 1

        engine = RealTimeEngine(keep_history=True)

        error_message = Feedback(engine)

        start_time = datetime.datetime.now()
        end_time = start_time + datetime.timedelta(seconds=5)

        action = [
            (start_time + datetime.timedelta(seconds=1), True),
            (start_time + datetime.timedelta(seconds=2), True),
            (start_time + datetime.timedelta(seconds=3), True),
            (start_time + datetime.timedelta(seconds=4), True),
        ]

        place = PlaceOrder(error_message, RealTimeDataSource('', engine, action))

        error_message << place.filter(lambda _, v: v == 2).map(lambda _, v: 'error')

        # engine.show_graph(show_cycle=True)
        engine.start(start_time, end_time)

        # print(place())
        # print(error_message())

        e = error_message()
        p = place()

        self.assertEqual(len(e), 1)

        t, message = e[0]
        self.assertEqual(message, 'error')

        before_error_time = [t for t, v in p if v == 2][0]
        before_error_index = [i for i, (t, v) in enumerate(p) if v == 2][0]

        self.assertEqual(t, before_error_time + datetime.timedelta(microseconds=1))

        t, v = p[before_error_index + 1]
        self.assertEqual(t, before_error_time + datetime.timedelta(microseconds=1))
        self.assertEqual(v, 'error')

    def testSnap(self):
        engine = RealTimeEngine(keep_history=True)
        now = datetime.datetime.now()
        snap_time = now + datetime.timedelta(seconds=1.5)
        values = RealTimeDataSource('test', engine, [
            (now + datetime.timedelta(seconds=1), 1),
            (now + datetime.timedelta(seconds=2), 2),
            (now + datetime.timedelta(seconds=3), 3),
        ])
        s = values.snap(snap_time)
        engine.start(now, now + datetime.timedelta(seconds=4))
        print(s())
        self.assertEqual(s()[0][1], 1)

        engine = RealTimeEngine(keep_history=True)
        now = datetime.datetime.now()
        snap_time = now + datetime.timedelta(seconds=2.5)
        values = RealTimeDataSource('test', engine, [
            (now + datetime.timedelta(seconds=1), 1),
            (now + datetime.timedelta(seconds=2), 2),
            (now + datetime.timedelta(seconds=3), 3),
        ])
        s = values.snap(snap_time)
        engine.start(now, now + datetime.timedelta(seconds=4))
        print(s())
        self.assertEqual(s()[0][1], 2)

        engine = RealTimeEngine(keep_history=True)
        now = datetime.datetime.now()
        snap_time = now + datetime.timedelta(seconds=0.5)
        values = RealTimeDataSource('test', engine, [
            (now + datetime.timedelta(seconds=1), 1),
            (now + datetime.timedelta(seconds=2), 2),
            (now + datetime.timedelta(seconds=3), 3),
        ])
        s = values.snap(snap_time)
        engine.start(now, now + datetime.timedelta(seconds=4))
        print(s())
        self.assertEqual(s(), [])

        engine = RealTimeEngine(keep_history=True)
        now = datetime.datetime.now()
        snap_time = now + datetime.timedelta(seconds=1)
        values = RealTimeDataSource('test', engine, [
            (now + datetime.timedelta(seconds=2), 1),
            (now + datetime.timedelta(seconds=3), 2),
            (now + datetime.timedelta(seconds=4), 3),
        ])
        s = values.snap(snap_time)
        engine.start(now, now + datetime.timedelta(seconds=4))
        print(s())
        self.assertEqual(s(), [])



import unittest

from datetime import datetime
from datetime import time
from datetime import timedelta
from flow.core import Engine, DataSource, Flow, when, Input, Feedback, Constant, Timer, Output, lift, DynamicFlow, \
    graph, Graph, flatten, Sample, SampleMethod, flow_to_dict, FixedTimer

import logging

logging.basicConfig(level=logging.DEBUG)


class TestFlow(unittest.TestCase):
    def testDependency(self):
        t1 = datetime(2016, 8, 1, 10, 11, 12)
        t2 = datetime(2016, 8, 1, 10, 11, 13)

        engine = Engine(keep_history=True)
        data1 = DataSource(engine, [[t1, 1], [t2, 2]], 'data1')
        data2 = DataSource(engine, [[t1, 3], [t2, 4]], 'data2')

        class Transform(Flow):
            input1 = Input()
            input2 = Input()

            output1 = Output()
            output2 = Output()

            def __init__(self, input1, input2, name):
                super().__init__(name)

            @when(input1)
            def handle(self):
                self.output1 = self.input1()

            @when(input2)
            def handle(self):
                self.output2 = self.input2()

        tx1 = Transform(data1, data2, 'transform1')
        tx2 = Transform(tx1.output1, tx1.output2, 'transform2')

        # engine.show_graph('dep', show_edge_label=True)
        engine.start(t1, t2)

        self.assertEqual(tx2.get_output_value()[tx2.output1], [(t1, 1), (t2, 2)])
        self.assertEqual(tx2.get_output_value()[tx2.output2], [(t1, 3), (t2, 4)])

    def testSanity(self):
        t1 = datetime(2016, 8, 1, 10, 11, 12)
        t2 = datetime(2016, 8, 1, 10, 11, 13)

        engine = Engine(keep_history=True)
        input_series = [(t1, 1), (t2, 2)]

        data = DataSource(engine, input_series)

        # class Transform(Flow):
        #     input = Input()
        #
        #     def __init__(self, input, name):
        #         super().__init__(name)
        #
        #     @when(input)
        #     def handle(self):
        #         self << self.input() + 1

        @lift()
        def add(i):
            return i + 1

        tx1 = add(data)
        tx2 = add(tx1)

        engine.start(t1, t2)

        self.assertEqual(tx1(), [(datetime(2016, 8, 1, 10, 11, 12), 2), (datetime(2016, 8, 1, 10, 11, 13), 3)])
        self.assertEqual(tx2(), [(datetime(2016, 8, 1, 10, 11, 12), 3), (datetime(2016, 8, 1, 10, 11, 13), 4)])

    # def testVWAP(self):
    #     class Trade:
    #         def __init__(self, price, shares):
    #             self.price = price
    #             self.shares = shares
    #
    #         def __str__(self):
    #             return 'Trade(price={}, shares={}'.format(self.price, self.shares)
    #
    #         def __repr__(self):
    #             return str(self)
    #
    #     engine = Engine()
    #
    #     n = 10
    #     start_time = datetime(2015, 1, 6, 1, 10, 12)
    #
    #     raw_trades = [Trade(i, i * 100) for i in range(1, n)]
    #     ts = [start_time + timedelta(seconds=i) for i in range(1, n)]
    #     trades = list(zip(ts, raw_trades))
    #
    #     trades = DataSource(engine, trades)
    #     prices = trades.price
    #     shares = trades.shares
    #
    #     vwap = (prices * shares).sum() / shares.sum()
    #
    #     # engine.show_graph()
    #
    #     engine.start(start_time, start_time + timedelta(seconds=n - 1))
    #
    #     expected = sum([trade.shares * trade.price for trade in raw_trades]) / sum([trade.shares for trade in raw_trades])
    #
    #     _, result = vwap()[0]
    #
    #     self.assertEqual(result, expected)

    def testFeedback(self):

        class N(Flow):
            input1 = Input()
            input2 = Input()

            def __init__(self, input1, input2):
                super().__init__()

            @when(input1, input2)
            def h(self):
                if self.input1 :
                    self << self.input1() + 1
                else:
                    self << self.input2()

        engine = Engine()

        n_1 = Feedback(engine)
        n = N(n_1, Constant(1, engine))

        n_1 << n

        # engine.show_graph(show_cycle=True)
        engine.start(datetime(2016, 1, 1, 1, 1, 1, 0), datetime(2016, 1, 1, 1, 1, 1, 5))

        self.assertEqual(n()[0][0], datetime(2016, 1, 1, 1, 1, 1, 5))
        self.assertEqual(n()[0][1], 6)

    def testTimer2(self):
        engine = Engine(keep_history=True)

        t1 = datetime(2016, 8, 1, 10, 11, 12)
        t2 = datetime(2016, 8, 2, 10, 11, 13)

        input1 = [(t1, 1)]
        input2 = [(t2, 2)]

        input1 = DataSource(engine, input1)
        input2 = DataSource(engine, input2)

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
                self.timer = self.now() + timedelta(minutes=60)

            @when(input2)
            def handle(self):
                if not self.done:
                    self << self.input1() * self.input2()

            @when(timer)
            def do_timer(self):
                if not self.input2 and not self.done:
                    self << self.input1() * self.default_value
                    self.done = True

        t = Transform(input1, input2, 10)

        engine.start(t1, t2)
        # engine.show_graph('timer')
        self.assertEqual(t(), [(t1 + timedelta(minutes=60), 10)])

    def testTimer(self):
        engine = Engine(keep_history=True)

        class T(Flow):
            input = Input()
            timer = Timer()

            def __init__(self, input):
                super().__init__()

            @when(input)
            def handle(self):
                self.timer = 5

            @when(timer)
            def do_timer(self):
                self << 1
                self.timer = 4

        start = Constant(1, engine)

        t = T(start)

        engine.start(datetime(2016, 1, 1, 1, 1, 1), datetime(2016, 1, 1, 1, 1, 10))
        self.assertEqual(t(), [(datetime(2016, 1, 1, 1, 1, 6), 1), (datetime(2016, 1, 1, 1, 1, 10), 1)])

    def testInheritance(self):
        engine = Engine(keep_history=True)

        class T(Flow):
            input = Input()

            def __init__(self, input):
                super().__init__()

            @when(input)
            def handle(self):
                self << self.compute(self.input())

            def compute(self, i):
                return i + 1

        class T2(T):
            def compute(self, i):
                return i + 3

        start = Constant(1, engine)

        t1 = T(start)
        t2 = T2(t1)

        ts = datetime(2016, 1, 1, 1, 1, 1)
        engine.start(ts, ts)

        t, v = t1()[0]
        self.assertEqual(t, ts)
        self.assertEqual(v, 2)

        t, v = t2()[0]
        self.assertEqual(t, ts)
        self.assertEqual(v, 5)

    def testDynamic(self):
        engine = Engine(keep_history=True)

        class T(DynamicFlow):
            def when(self, input1, input2):
                if input1.is_active() and input2.is_active():
                    self << input1() + input2()

        input1 = Constant(1, engine)
        input2 = Constant(2, engine)

        t = T(input1, input2)

        ts = datetime(2016, 1, 1, 1, 1, 1)
        engine.start(ts, ts)

        t, v = t()[0]
        self.assertEqual(t, ts)
        self.assertEqual(v, 3)

    def testWhenOrder(self):
        engine = Engine(keep_history=True)

        class Test(Flow):
            in1 = Input()
            in2 = Input()

            def __init__(self, in1, in2):
                super().__init__()
                self.cache = None

            @when(in2)
            def handle(self):
                self.cache = 1

            @when(in2, in1)
            def when(self):
                self << self.cache

        input1 = Constant(1, engine)
        input2 = Constant(2, engine)

        t = Test(input1, input2)

        ts = datetime(2016, 1, 1, 1, 1, 1)
        engine.start(ts, ts)

        t, v = t()[0]
        self.assertEqual(v, 1)

    def testGrouping(self):
        engine = Engine(keep_history=True)

        class Test(Flow):
            in1 = Input()
            in2 = Input()

            def __init__(self, in1, in2):
                super().__init__('test')
                self.cache = None

            @when(in2)
            def handle(self):
                self.cache = 1

            @when(in2, in1)
            def when(self):
                self << self.cache

        @graph('group1')
        def group1():
            input1 = Constant(1, engine)
            input2 = Constant(2, engine)

            return Test(input1, input2)

        @graph('group2')
        def group2():
            input1 = Constant(1, engine)
            input2 = Constant(2, engine)

            @graph('graph2.1')
            def group22():
                input1 = Constant(3, engine)
                input2 = Constant(4, engine)

                return Test(input1, input2)

            return Test(input1, input2) + group22()

        @graph('group3')
        def group3(input1, input2):
            return input1 + input2

        ts = datetime(2016, 1, 1, 1, 1, 1)
        g1 = group1()
        g2 = group2()
        g3 = group3(g1, g2)

        graphs = list()

        def print_it(o1, o2):
            graphs.append((str(o1), str(o2)))

        engine.traverse(print_it)

        expected = [
            ('group1', 'root'),
            ('1', 'group1'),
            ('2', 'group1'),
            ('test', 'group1'),
            ('group2', 'root'),
            ('1', 'group2'),
            ('2', 'group2'),
            ('test', 'group2'),
            ('graph2.1', 'group2'),
            ('3', 'graph2.1'),
            ('4', 'graph2.1'),
            ('test', 'graph2.1'),
            ('add(+)', 'group2'),
            ('group3', 'root'),
            ('add(+)', 'group3'),
        ]
        self.assertEqual(expected, graphs)

    def testFlatten1(self):
        t1 = datetime(2016, 1, 1, 1, 1, 1)
        t2 = datetime(2016, 1, 1, 1, 1, 2)
        t4 = datetime(2016, 1, 1, 1, 1, 4)

        engine = Engine(keep_history=True)

        d1 = DataSource(engine, [
            (t1, 1),
            (t2, 2),
            (t4, 5)
        ])

        out = flatten([d1])

        engine.start(t1, t4)

        t, v = out()[0]
        self.assertEqual(t, t1)
        self.assertEqual(v, [1])

        t, v = out()[1]
        self.assertEqual(t, t2)
        self.assertEqual(v, [2])

        t, v = out()[2]
        self.assertEqual(t, t4)
        self.assertEqual(v, [5])

    def testFlatten2(self):
        t1 = datetime(2016, 1, 1, 1, 1, 1)
        t2 = datetime(2016, 1, 1, 1, 1, 2)
        t3 = datetime(2016, 1, 1, 1, 1, 3)
        t4 = datetime(2016, 1, 1, 1, 1, 4)

        engine = Engine(keep_history=True)

        d1 = DataSource(engine, [
            (t1, 1),
            (t2, 2),
            (t4, 5)
        ])

        d2 = DataSource(engine, [
            (t1, 3),
            (t3, 4),
            (t4, 6)
        ])

        out = flatten([d1, d2])

        engine.start(t1, t4)

        t, v = out()[0]
        self.assertEqual(t, t1)
        self.assertEqual(v, [1, 3])

        t, v = out()[1]
        self.assertEqual(t, t2)
        self.assertEqual(v, [2])

        t, v = out()[2]
        self.assertEqual(t, t3)
        self.assertEqual(v, [4])

        t, v = out()[3]
        self.assertEqual(t, t4)
        self.assertEqual(v, [5, 6])

    def testSampleFirst(self):
        values = [(datetime(2016, 1, 1, 1, 1, i * 2), i * 2) for i in range(1, 20)]

        engine = Engine(keep_history=True)

        d = DataSource(engine, values)
        s = d.sample(timedelta(seconds=7), method=SampleMethod.First)

        engine.start(values[0][0], values[-1][0])

        first_result = s()

        first_expected = [
            (datetime(2016, 1, 1, 1, 1, 9), 2),
            (datetime(2016, 1, 1, 1, 1, 16), 10),
            (datetime(2016, 1, 1, 1, 1, 23), 16),
            (datetime(2016, 1, 1, 1, 1, 30), 24),
            (datetime(2016, 1, 1, 1, 1, 37), 30),
        ]
        self.assertEqual(first_expected, first_result)

    def testSampleLast(self):
        values = [(datetime(2016, 1, 1, 1, 1, i * 2), i * 2) for i in range(1, 20)]

        engine = Engine(keep_history=True)

        d = DataSource(engine, values)
        s = d.sample(timedelta(seconds=7), method=SampleMethod.Last)

        engine.start(values[0][0], values[-1][0])

        result = s()

        expected = [
            (datetime(2016, 1, 1, 1, 1, 9), 8),
            (datetime(2016, 1, 1, 1, 1, 16), 14),
            (datetime(2016, 1, 1, 1, 1, 23), 22),
            (datetime(2016, 1, 1, 1, 1, 30), 28),
            (datetime(2016, 1, 1, 1, 1, 37), 36),
        ]

        self.assertEqual(expected, result)

    def testSampleFirstWithStartTime(self):
        values = [(datetime(2016, 1, 1, 1, 1, i * 2), i * 2) for i in range(1, 20)]

        engine = Engine(keep_history=True)

        d = DataSource(engine, values)
        s = d.sample(timedelta(seconds=7), method=SampleMethod.First, start_time=values[2][0])

        engine.start(values[0][0], values[-1][0])

        first_result = s()

        first_expected = [
            (datetime(2016, 1, 1, 1, 1, 6, 1), 6),
            (datetime(2016, 1, 1, 1, 1, 13, 1), 8),
            (datetime(2016, 1, 1, 1, 1, 20, 1), 14),
            (datetime(2016, 1, 1, 1, 1, 27, 1), 22),
            (datetime(2016, 1, 1, 1, 1, 34, 1), 28),
        ]
        self.assertEqual(first_expected, first_result)

    def testSampleLastWithStartTime(self):
        values = [(datetime(2016, 1, 1, 1, 1, i * 2), i * 2) for i in range(1, 20)]

        engine = Engine(keep_history=True)

        d = DataSource(engine, values)
        print(values[2][0])
        s = d.sample(timedelta(seconds=7), method=SampleMethod.Last, start_time=values[2][0])

        engine.start(values[0][0], values[-1][0])

        result = s()

        expected = [
            (datetime(2016, 1, 1, 1, 1, 6, 1), 6),
            (datetime(2016, 1, 1, 1, 1, 13, 1), 12),
            (datetime(2016, 1, 1, 1, 1, 20, 1), 20),
            (datetime(2016, 1, 1, 1, 1, 27, 1), 26),
            (datetime(2016, 1, 1, 1, 1, 34, 1), 34),
        ]

        self.assertEqual(expected, result)

    def testShift(self):
        values = [(datetime(2016, 1, 1, 1, 1, i), i) for i in range(1, 10)]

        engine = Engine(keep_history=True)

        d = DataSource(engine, values)
        s = d.shift(2)

        engine.start(values[0][0], values[-1][0])

        result = s()
        for t, v in result:
            self.assertEqual(t.second, v + 2)

        # expected = [
        #     (datetime(2016, 1, 1, 1, 1, 9), 8),
        #     (datetime(2016, 1, 1, 1, 1, 16), 14),
        #     (datetime(2016, 1, 1, 1, 1, 23), 22),
        #     (datetime(2016, 1, 1, 1, 1, 30), 28),
        #     (datetime(2016, 1, 1, 1, 1, 37), 36),
        # ]
        #
        # self.assertEqual(expected, result)

    def testFlatten3(self):
        t0 = datetime(2016, 1, 1, 1, 1, 0)
        t1 = datetime(2016, 1, 1, 1, 1, 1)
        t2 = datetime(2016, 1, 1, 1, 1, 2)
        t3 = datetime(2016, 1, 1, 1, 1, 3)
        t4 = datetime(2016, 1, 1, 1, 1, 4)
        t5 = datetime(2016, 1, 1, 1, 1, 5)

        engine = Engine(keep_history=True)

        d1 = DataSource(engine, [
            (t0, 0.5),
            (t1, 1),
            (t2, 2),
            (t4, 5)
        ])

        d2 = DataSource(engine, [
            (t1, 3),
            (t3, 4),
            (t4, 6)
        ])

        d3 = DataSource(engine, [
            (t1, 9),
            (t3, 10),
            (t5, 11),
        ])

        out = flatten([d1, d2, d3], is_fill_empty=True)
        engine.start(t0, t5)
        o = out()
        print(o)

        t, v = o[1]
        self.assertEqual(t, t1)
        self.assertEqual([1, 3, 9], v)

        t, v = o[2]
        self.assertEqual(t, t2)
        self.assertEqual([2, None, None], v)

        t, v = o[3]
        self.assertEqual(t, t3)
        self.assertEqual([None, 4, 10], v)

        t, v = o[4]
        self.assertEqual(t, t4)
        self.assertEqual([5, 6, None], v)

        t, v = o[5]
        self.assertEqual(t, t5)
        self.assertEqual([None, None, 11], v)

    def testToDict(self):
        t0 = datetime(2016, 1, 1, 1, 1, 0)
        t1 = datetime(2016, 1, 1, 1, 1, 1)
        t2 = datetime(2016, 1, 1, 1, 1, 2)
        t3 = datetime(2016, 1, 1, 1, 1, 3)
        t4 = datetime(2016, 1, 1, 1, 1, 4)
        t5 = datetime(2016, 1, 1, 1, 1, 5)

        engine = Engine(keep_history=True)

        d1 = DataSource(engine, [
            (t0, 0.5),
            (t1, 1),
            (t2, 2),
            (t4, 5)
        ])

        d2 = DataSource(engine, [
            (t1, 3),
            (t3, 4),
            (t4, 6)
        ])

        d3 = DataSource(engine, [
            (t1, 9),
            (t3, 10),
            (t5, 11),
        ])

        out = flow_to_dict({'d1': d1, 'd2': d2, 'd3': d3})
        engine.start(t0, t5)
        o = out()

        t, v = o[0]
        self.assertEqual(t, t0)
        self.assertEqual(dict(d1=0.5), v)

        t, v = o[1]
        self.assertEqual(t, t1)
        self.assertEqual(dict(d1=1, d2=3, d3=9), v)

        t, v = o[2]
        self.assertEqual(t, t2)
        self.assertEqual(dict(d1=2, d2=3, d3=9), v)

        t, v = o[3]
        self.assertEqual(t, t3)
        self.assertEqual(dict(d1=2, d2=4, d3=10), v)

        t, v = o[4]
        self.assertEqual(t, t4)
        self.assertEqual(dict(d1=5, d2=6, d3=10), v)

        t, v = o[5]
        self.assertEqual(t, t5)
        self.assertEqual(dict(d1=5, d2=6, d3=11), v)

    def testRolling(self):
        t0 = datetime(2016, 1, 1, 1, 1, 0)
        t1 = datetime(2016, 1, 1, 1, 1, 1)
        t2 = datetime(2016, 1, 1, 1, 1, 2)
        t3 = datetime(2016, 1, 1, 1, 1, 3)
        t4 = datetime(2016, 1, 1, 1, 1, 4)
        t5 = datetime(2016, 1, 1, 1, 1, 5)
        t6 = datetime(2016, 1, 1, 1, 1, 8)
        t7 = datetime(2016, 1, 1, 1, 1, 8, 5)

        engine = Engine(keep_history=True)

        d = DataSource(engine, [
            (t0, 0),
            (t1, 1),
            (t2, 2),
            (t3, 3),
            (t4, 4),
            (t5, 5),
            (t6, 6),
            (t7, 7),
        ])

        window = timedelta(seconds=2)
        out = d.rolling(window)
        engine.start(t0, t7)
        o = out()

        self.assertEqual(len(o), 8)

        t, v = o[0]
        self.assertEqual(t0, t)
        self.assertEqual([0], v)

        t, v = o[1]
        self.assertEqual(t1, t)
        self.assertEqual([0, 1], v)

        t, v = o[2]
        self.assertEqual(t2, t)
        self.assertEqual([0, 1, 2], v)

        t, v = o[3]
        self.assertEqual(t3, t)
        self.assertEqual([1, 2, 3], v)

        t, v = o[4]
        self.assertEqual(t4, t)
        self.assertEqual([2, 3, 4], v)

        t, v = o[5]
        self.assertEqual(t5, t)
        self.assertEqual([3, 4, 5], v)

        t, v = o[6]
        self.assertEqual(t6, t)
        self.assertEqual([6], v)

        t, v = o[7]
        self.assertEqual(t7, t)
        self.assertEqual([6, 7], v)

    def testSnap(self):
        engine = Engine(keep_history=True)
        snap_time = datetime(2020, 11, 8, 4, 45, 30)
        values = DataSource(engine, [
            (datetime(2020, 11, 8, 4, 45), 1),
            (datetime(2020, 11, 8, 4, 46), 2),
            (datetime(2020, 11, 8, 4, 47), 3),
        ])
        s = values.snap(snap_time)
        engine.start(datetime(2020, 11, 8, 4, 44), datetime(2020, 11, 8, 4, 47))
        self.assertEqual(s(), [(datetime(2020, 11, 8, 4, 45, 30), 1)])

        engine = Engine(keep_history=True)
        snap_time = datetime(2020, 11, 8, 4, 46)
        values = DataSource(engine, [
            (datetime(2020, 11, 8, 4, 45), 1),
            (datetime(2020, 11, 8, 4, 46), 2),
            (datetime(2020, 11, 8, 4, 47), 3),
        ])
        s = values.snap(snap_time)
        engine.start(datetime(2020, 11, 8, 4, 44), datetime(2020, 11, 8, 4, 48))
        self.assertEqual(s(), [(datetime(2020, 11, 8, 4, 46), 2)])

        engine = Engine(keep_history=True)
        snap_time = datetime(2020, 11, 8, 4, 43)
        values = DataSource(engine, [
            (datetime(2020, 11, 8, 4, 45), 1),
            (datetime(2020, 11, 8, 4, 46), 2),
            (datetime(2020, 11, 8, 4, 47), 3),
        ])
        s = values.snap(snap_time)
        engine.start(datetime(2020, 11, 8, 4, 41), datetime(2020, 11, 8, 4, 48))
        # print(s())
        self.assertEqual(s(), None)

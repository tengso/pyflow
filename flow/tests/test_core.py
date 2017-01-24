import pandas as pd
import unittest

from datetime import datetime
from datetime import timedelta
from flow.core import Engine, DataSource, Flow, when, Input, Feedback, Constant, Timer, Output, lift


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

    def testVWAP(self):
        class Trade:
            def __init__(self, price, shares):
                self.price = price
                self.shares = shares

            def __str__(self):
                return 'Trade(price={}, shares={}'.format(self.price, self.shares)

            def __repr__(self):
                return str(self)

        engine = Engine()

        n = 10
        start_time = datetime(2015, 1, 6, 1, 10, 12)

        raw_trades = [Trade(i, i * 100) for i in range(1, n)]
        ts = [start_time + timedelta(seconds=i) for i in range(1, n)]
        trades = list(zip(ts, raw_trades))

        trades = DataSource(engine, trades)
        prices = trades.price
        shares = trades.shares

        vwap = (prices * shares).sum() / shares.sum()

        # engine.show_graph()

        engine.start(start_time, start_time + timedelta(seconds=n - 1))

        expected = sum([trade.shares * trade.price for trade in raw_trades]) / sum([trade.shares for trade in raw_trades])

        _, result = vwap()[0]

        self.assertEqual(result, expected)

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

    def testLifting(self):
        t1 = datetime(2016, 8, 1, 10, 11, 12)
        t2 = datetime(2016, 8, 1, 10, 11, 13)

        engine = Engine(keep_history=True)

        @lift()
        def zscore(p):
            return (p - p.mean()) / p.std()

        data = DataSource(engine, [
            (t1, pd.Series([1, 2, 3, 4])),
            (t2, pd.Series([2, 3, 4, 6]))
            ]
        )

        z = zscore(data)

        # engine.show_graph('test')
        engine.start(t1, t2)

        print(z())





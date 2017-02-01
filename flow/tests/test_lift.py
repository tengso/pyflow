import unittest
from datetime import datetime

from flow.core import Engine, DataSource, lift


class LiftTest(unittest.TestCase):
    def testLifting(self):
        t1 = datetime(2016, 8, 1, 10, 11, 12)
        t2 = datetime(2016, 8, 1, 10, 11, 13)

        engine = Engine(keep_history=True)

        @lift()
        def f(p):
            return True if p > 1 else False

        data = DataSource(engine, [
            (t1, 1),
            (t2, 2),
        ])

        z = f(data)

        # engine.show_graph('test')
        engine.start(t1, t2)

        self.assertEqual(len(z()), 2)

        (ts0, v0) = z()[0]
        (ts1, v1) = z()[1]

        self.assertEqual(ts0, t1)
        self.assertEqual(v0, False)

        self.assertEqual(ts1, t2)
        self.assertEqual(v1, True)

    def testLifting2(self):
        t1 = datetime(2016, 8, 1, 10, 11, 12)
        t2 = datetime(2016, 8, 1, 10, 11, 13)

        engine = Engine(keep_history=True)

        @lift()
        def sum(p1, p2):
            return p1 + p2

        @lift()
        def sum3(p1, p2, p3):
            return p1 + p2 + p3

        data_1 = DataSource(engine, [
            (t1, 1),
            (t2, 2),
        ])

        data_2 = DataSource(engine, [
            (t1, 1),
            (t2, 2),
        ])

        z = sum(data_1, data_2)
        result = sum3(z, z, z)

        # engine.show_graph('test')
        engine.start(t1, t2)

        result = result()

        self.assertEqual(len(result), 2)

        (ts0, v0) = result[0]
        (ts1, v1) = result[1]

        self.assertEqual(ts0, t1)
        self.assertEqual(v0, 6)

        self.assertEqual(ts1, t2)
        self.assertEqual(v1, 12)

    def testLifting3(self):
        t1 = datetime(2016, 8, 1, 10, 11, 12)
        t2 = datetime(2016, 8, 1, 10, 11, 13)
        t3 = datetime(2016, 8, 1, 10, 11, 14)

        engine = Engine(keep_history=True)

        @lift(timed=True, passive='p2')
        def sum(ts, p1, p2, p3):
            return ts, p1 + p2 + p3

        data_1 = DataSource(engine, [
            (t1, 1),
        ])

        data_2 = DataSource(engine, [
            (t2, 2),
        ])

        data_3 = DataSource(engine, [
            (t1, 1),
            (t3, 3),
        ])

        result = sum(data_1, data_2, data_3)

        # engine.show_graph('test')
        engine.start(t1, t3)

        result = result()

        self.assertEqual(len(result), 1)

        (ts0, (ts, v0)) = result[0]

        self.assertEqual(ts0, t3)
        self.assertEqual(v0, 6)
        self.assertEqual(ts, t3)


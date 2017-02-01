import unittest
from datetime import datetime

from flow.core import Engine, DataSource, Flow, Input


class TestOps(unittest.TestCase):
    def test(self):
        t1 = datetime(2016, 8, 1, 10, 11, 12)
        t2 = datetime(2016, 8, 1, 10, 11, 13)

        engine = Engine(keep_history=True)

        data1 = DataSource(engine, [
            (t1, 0),
            (t2, 2),
        ])

        data = data1 + 1
        data = 1 + data
        data = data * 1
        data = 1 * data
        data = data / 1
        data = 1 / data
        data = -data
        data = abs(data)

        # engine.show_graph('dep', show_edge_label=True)
        engine.start(t1, t2)
        self.assertEqual(len(data()), 2)

        t, v = data()[0]
        self.assertEqual(t, t1)
        self.assertEqual(v, 0.5)

        t, v = data()[1]
        self.assertEqual(t, t2)
        self.assertEqual(v, 0.25)



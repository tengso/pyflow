import time
from queue import Queue, Empty as QueueEmpty
from enum import Enum
from collections import OrderedDict
from threading import Thread
from typing import Sequence, Tuple, Any

import datetime
from datetime import timedelta
import logging
import heapq
import graphviz as gv
from inspect import signature

from abc import abstractmethod


class NodeRegistry(object):
    input_registry = {}
    output_registry = {}

    @staticmethod
    def clear():
        NodeRegistry.input_registry = {}
        NodeRegistry.output_registry = {}

    @staticmethod
    def get_input(owner, node_id):
        return NodeRegistry.input_registry.get((owner, node_id))

    @staticmethod
    def add_input(owner, node_id, node):
        NodeRegistry.input_registry[(owner, node_id)] = node

    @staticmethod
    def get_output(owner, node_id):
        return NodeRegistry.output_registry.get((owner, node_id))

    @staticmethod
    def add_output(owner, node_id, node):
        NodeRegistry.output_registry[(owner, node_id)] = node


class Graph(object):
    def __init__(self, name):
        self.name = name
        self.children = []

    def add_child(self, child):
        self.children.append(child)

    def get_children(self):
        return self.children

    def __str__(self):
        return self.name

    def __repr__(self):
        return str(self)


class GraphRoot(Graph):
    def __init__(self):
        super().__init__('root')


class GraphStack(object):
    stack = list()

    @staticmethod
    def push(graph):
        GraphStack.stack.insert(0, graph)

    @staticmethod
    def pop():
        if len(GraphStack.stack):
            n = GraphStack.stack[0]
            del GraphStack.stack[0]
            return n
        else:
            return None

    @staticmethod
    def peek():
        if len(GraphStack.stack):
            return GraphStack.stack[0]
        else:
            return None


class Node(object):
    def __init__(self, owner, node_id):
        self.owner = owner
        self.node_id = node_id

    def get_id(self):
        return self.node_id

    def get_owner(self):
        return self.owner

    def has_value(self):
        pass

    def get_last_value(self):
        pass

    def __call__(self):
        return self.get_last_value()


class OutputNode(Node):
    def __init__(self, owner, node_id, value_type=None):
        super().__init__(owner, node_id)
        self.children = []
        self.value_type = value_type
        # (time, value)
        self.values = []
        self.last_time = None
        self.keep_history = owner.get_engine().keep_history

    def get_value_type(self):
        return self.value_type

    def get_children(self):
        return self.children

    def add_child(self, child):
        self.children.append(child)

    def get_last_value(self):
        v = self.get_last_value_with_time()
        if v is not None:
            return v[1]
        else:
            return None

    def get_last_value_with_time(self):
        if self.has_value():
            return self.values[-1]
        else:
            return None

    def has_value(self):
        return len(self.values) > 0

    def add_value(self, now, value):
        if self.last_time and now <= self.last_time:
            raise RuntimeError('{} is not later than {}'.format(now, self.last_time))

        # TODO: use a more efficient data structure for values
        if self.keep_history or not len(self.values):
            self.values.append((now, value))
        else:
            self.values[0] = (now, value)

        self.last_time = now

    def get_all_value(self):
        return self.values

    def is_active(self, timestamp):
        # return self.last_time and self.last_time == timestamp
        return self.last_time == timestamp


class InputNode(Node):
    def __init__(self, owner, node_id, parent, value_type=None):
        super().__init__(owner, node_id)
        self.parent = parent
        self.value_type = value_type

    def get_value_type(self):
        return self.value_type

    def set_parent(self, parent):
        self.parent = parent

    def get_parent(self):
        return self.parent

    def get_last_value(self):
        return self.get_parent()()

    def has_value(self):
        return self.get_parent().has_value()

    def is_active(self):
        now = self.owner.get_engine().now()
        return self.parent.is_active(now)

    def __bool__(self):
        return self.has_value()


class WhenBlockRegistry:
    registry = OrderedDict()
    code_cache = {}

    @staticmethod
    def add(input_id, code):
        if code not in WhenBlockRegistry.registry:
            WhenBlockRegistry.registry[code] = []
        WhenBlockRegistry.registry[code].append(input_id)

    @staticmethod
    def get_code_list(input_ids):
        input_ids_key = str(input_ids)

        code_list = WhenBlockRegistry.code_cache.get(input_ids_key)
        if code_list:
            return code_list

        code_list = []
        for code, ids in WhenBlockRegistry.registry.items():
            for input_id in input_ids:
                if input_id in ids and code not in code_list:
                    code_list.append(code)

        WhenBlockRegistry.code_cache[input_ids_key] = code_list
        return code_list


class when:
    def __init__(self, *inputs):
        self.inputs = inputs

    def __call__(self, f):
        for input in self.inputs:
            WhenBlockRegistry.add(id(input), f)


class Input:
    def __init__(self, value_type=None):
        self.value_type = value_type

    def __get__(self, instance, owner):
        # print('input get instance: {} owner: {}'.format(instance, owner))

        node = NodeRegistry.get_input(instance, id(self))
        if node is None:
            raise RuntimeError('{}: {} not registered'.format(instance, self))
        return node

    def __set__(self, instance, value):
        # print('input get instance: {} value: {}'.format(instance, value))

        if isinstance(value, FlowBase):
            value = value._output

        node = NodeRegistry.get_input(instance, id(self))
        if node is None:
            node = InputNode(instance, id(self), value, self.value_type)
            NodeRegistry.add_input(instance, id(self), node)

        if node.get_value_type() and value.get_value_type() and not issubclass(value.get_value_type(), node.get_value_ype()):
            raise TypeError('output type {} is not subclass of input type {}'.format(value.get_value_type(), node.get_value_type()))

        value.add_child(node)
        instance.add_input(node)


class Timer(Input):
    def __set__(self, instance, value):
        super().__init__()
        # print('input set instance: {} value: {}'.format(instance, value))

        now = instance.now()
        engine = instance.get_engine()

        time_up = value if isinstance(value, datetime.datetime) else now + value if isinstance(value, datetime.timedelta) \
            else now + datetime.timedelta(seconds=value)

        time_up = time_up if time_up > now else now + timedelta(microseconds=engine.get_interval())

        if isinstance(engine, RealTimeEngine):
            timer = RealTimeFixedTimer(engine, [time_up])
            timer.start(None, None)
        else:
            timer = FixedTimer(engine, [time_up])

        node = NodeRegistry.get_input(instance, id(self))

        if node is None:
            node = InputNode(instance, id(self), timer._output)
            NodeRegistry.add_input(instance, id(self), node)
        else:
            parent = node.get_parent()
            if parent is not None:
                engine.remove_source(parent.get_owner())
            node.set_parent(timer._output)

        timer._output.add_child(node)
        instance.add_input(node)


class Output:
    def __init__(self, value_type=None):
        self.value_type = value_type

    def __get__(self, instance, cls):
        node = NodeRegistry.get_output(instance, id(self))
        if node is None:
            node = OutputNode(instance, id(self), self.value_type)
            NodeRegistry.add_output(instance, id(self), node)

        instance.add_output(node)
        return node

    def __set__(self, instance, value):
        node = NodeRegistry.get_output(instance, id(self))
        if node is None:
            node = OutputNode(instance, id(self), self.value_type)
            NodeRegistry.add_output(instance, id(self), node)
            instance.add_output(node)

        now = instance.get_engine().now()
        node.add_value(now, value)

    def __lshift__(self, other):
        pass


def init_wrapper(orig_init, input_names, positions):
    def wrapper(*args, **kwargs):
        orig_init(*args, **kwargs)

        instance = args[0]

        for name, value in kwargs.items():
            if name in input_names:
                if not (isinstance(value, OutputNode) or isinstance(value, FlowBase)):
                    raise TypeError('{} is not type output'.format(value))
                setattr(instance, name, value)

        for index, arg in enumerate(args):
            if index in positions:
                if not (isinstance(arg, OutputNode) or isinstance(arg, FlowBase)):
                    raise TypeError('{} with type {} is not of type Output'.format(arg, type(arg)))
                setattr(instance, positions[index], arg)

    return wrapper


class MetaFlow(type):
    def __new__(mcs, cls_name, bases, cls_dict):
        # TODO: reference class
        if cls_name != 'Feedback':
            input_names = set()
            for name, value in cls_dict.items():
                if issubclass(value.__class__, Input) and not issubclass(value.__class__, Timer):
                    input_names.add(name)

            positions = {}
            for name, value in cls_dict.items():
                if name == '__init__':
                    params = signature(value).parameters
                    for index, param in enumerate(params):
                        if param in input_names:
                            positions[index] = param

                    cls_dict[name] = init_wrapper(value, input_names, positions)

            if len(positions) != len(input_names):
                raise TypeError('parameters and inputs mismatch {} vs. {}.'.format(positions, input_names))

        return super().__new__(mcs, cls_name, bases, cls_dict)


class FlowOps:
    # def __getattr__(self, item):
    #     # TODO: better check
    #     if self.__class__ != Flow:
    #         self.warn('created map for {}:{}', self, item)
    #         return MapN(item, lambda value: getattr(value, item), self)

    def __mul__(self, other):
        if not isinstance(other, FlowBase):
            engine = self.get_engine()
            if isinstance(engine, RealTimeEngine):
                other = RealTimeConstant(other, engine)
            else:
                other = Constant(other)
                engine.add_source(other)

        return MapN('mul(*)', lambda input1, input2: input1 * input2, self, other)

    def __rmul__(self, other):
        if not isinstance(other, FlowBase):
            engine = self.get_engine()
            if isinstance(engine, RealTimeEngine):
                other = RealTimeConstant(other, engine)
            else:
                other = Constant(other)
                engine.add_source(other)

        return MapN('mul(*)', lambda input1, input2: input1 * input2, other, self)

    def __sub__(self, other):
        if not isinstance(other, FlowBase):
            engine = self.get_engine()
            if isinstance(engine, RealTimeEngine):
                other = RealTimeConstant(other, engine)
            else:
                other = Constant(other)
                engine.add_source(other)

        return MapN('sub(-)', lambda input1, input2: input1 - input2, self, other)

    def __rsub__(self, other):
        if not isinstance(other, FlowBase):
            engine = self.get_engine()
            if isinstance(engine, RealTimeEngine):
                other = RealTimeConstant(other, engine)
            else:
                other = Constant(other)
                engine.add_source(other)

        return MapN('sub(-)', lambda input1, input2: input1 - input2, other, self)

    def __add__(self, other):
        if not isinstance(other, FlowBase):
            engine = self.get_engine()
            if isinstance(engine, RealTimeEngine):
                other = RealTimeConstant(other, engine)
            else:
                other = Constant(other)
                engine.add_source(other)

        return MapN('add(+)', lambda input1, input2: input1 + input2, self, other)

    def __radd__(self, other):
        if not isinstance(other, FlowBase):
            engine = self.get_engine()
            if isinstance(engine, RealTimeEngine):
                other = RealTimeConstant(other, engine)
            else:
                other = Constant(other)
                engine.add_source(other)

        return MapN('add(+)', lambda input1, input2: input1 + input2, other, self)

    def __pow__(self, other):
        if not isinstance(other, FlowBase):
            engine = self.get_engine()
            if isinstance(engine, RealTimeEngine):
                other = RealTimeConstant(other, engine)
            else:
                other = Constant(other)
                engine.add_source(other)

        return MapN('pow(**)', lambda input1, input2: input1 ** input2, self, other)

    def __abs__(self):
        return MapN('abs', lambda input: abs(input), self)

    def __neg__(self):
        return MapN('neg(-)', lambda input: -input, self)

    def __pos__(self):
        return MapN('pos(+)', lambda input: +input, self)

    def __truediv__(self, other):
        if not isinstance(other, FlowBase):
            engine = self.get_engine()
            if isinstance(engine, RealTimeEngine):
                other = RealTimeConstant(other, engine)
            else:
                other = Constant(other)
                engine.add_source(other)

        return MapN('div(/)', lambda input1, input2: input1 / input2, self, other)

    def __rtruediv__(self, other):
        if not isinstance(other, FlowBase):
            engine = self.get_engine()
            if isinstance(engine, RealTimeEngine):
                other = RealTimeConstant(other, engine)
            else:
                other = Constant(other)
                engine.add_source(other)

        return MapN('div(/)', lambda input1, input2: input1 / input2, other, self)

    def __lt__(self, other):
        return self.compare(other, lambda in1, in2: in1 < in2, 'lt(<)')

    def __le__(self, other):
        return self.compare(other, lambda in1, in2: in1 <= in2, 'le(<=)')

    def __gt__(self, other):
        return self.compare(other, lambda in1, in2: in1 > in2, 'gt(>)')

    def __ge__(self, other):
        return self.compare(other, lambda in1, in2: in1 >= in2, 'ge(>=)')

    def __len__(self, other):
        pass

    def compare(self, other, fun, name='compare'):
        if not isinstance(other, FlowBase):
            engine = self.get_engine()
            if isinstance(engine, RealTimeEngine):
                other = RealTimeConstant(other, engine)
            else:
                other = Constant(other)
                engine.add_source(other)

        return MapN(name, lambda input1, input2: fun(input1, input2), self, other)

    def filter(self, filter_fun, name='filter'):
        return Filter(self, filter_fun, name)

    def filter_by(self, filter_value, filter_fun, name='filter_by'):
        return FilterBy(self, filter_value, filter_fun, name)

    def restrict(self, restrict_fun):
        return Restrict(self, restrict_fun)

    def sum(self):
        return Fold(self, 0, lambda accum, i: accum + i, 'cumsum')

    def product(self):
        return Fold(self, 1, lambda accum, i: accum * i, 'product')

    def count(self):
        return Fold(self, 0, lambda accum, i: accum + 1, 'count')

    def ignore_small_move(self, small_move):
        class Ignore(Flow):
            input = Input()

            def __init__(self, input):
                super().__init__('ignore small move')
                self.last = None

            @when(input)
            def ignore(self):
                if not self.last or abs(self.input() - self.last) > small_move:
                    self << self.input()
                    self.last = self.input()

        return Ignore(self)

    def flatten(self, inputs, is_fill_empty=False):
        if not isinstance(inputs, list):
            input_list = [inputs]
        else:
            input_list = inputs

        return flatten(input_list + [self], is_fill_empty=is_fill_empty)

    def probe(self, msg='{}'):
        def l(i):
            self.info(msg.format(i))
            return i

        return MapN('probe', l, self)

    def fold(self, init, accum):
        return Fold(self, init, accum)

    def map(self, map_fun, name='map'):
        return MapN(name, map_fun, self, timed=True)

    def sample(self, interval: timedelta, method, start_time=None):
        engine = self.get_engine()
        if start_time is not None:
            if isinstance(engine, RealTimeEngine):
                start = RealTimeFixedTimer(engine, [start_time])
            else:
                start = FixedTimer(engine, [start_time])
        else:
            if isinstance(engine, RealTimeEngine):
                start = RealTimeEmpty(engine)
            else:
                start = Empty(engine)

        return Sample(self, start, interval, method, is_auto_start=start_time is None)

    def fast_sample(self):
        return FastSample(self)

    def shift(self, shift):
        return Shift(self, shift)

    def wait(self, other):
        w = Wait2(self, other)
        return MapN('wait', lambda v: v, w.o1), MapN('wait', lambda v: v, w.o2)

    def snap(self, asof: datetime.datetime):
        engine = self.get_engine()
        timer = RealTimeFixedTimer(engine, timestamps=[asof]) if isinstance(engine, RealTimeEngine) else FixedTimer(engine, timestamps=[asof])
        return Snap(self, timer)

    def rolling(self, window: datetime.timedelta):
        return Rolling(self, window)

    def until(self, asof: datetime.datetime):
        return Until(self, asof)

    def start(self, asof: datetime.datetime):
        return Start(self, asof)


class FlowBase(FlowOps):
    _output = Output()

    def __init__(self, name=None):
        self.inputs = []
        self.outputs = []
        self.engine = None
        self.name = name
        GraphStack.peek().add_child(self)

    def get_logger(self):
        # have to use lazy-initialization here to work with Spark
        return logging.getLogger(self.name)

    def get_name(self):
        return self.name

    def add_input(self, input):
        if input not in self.inputs:
            self.inputs.append(input)

    def add_output(self, output):
        if output not in self.outputs:
            self.outputs.append(output)

    def get_inputs(self):
        return self.inputs

    def get_outputs(self):
        return self.outputs

    def get_children(self, skip_feedback=False):
        children = set()

        for output in self.outputs:
            for child in output.get_children():
                owner = child.get_owner()
                if not skip_feedback or not isinstance(owner, Feedback):
                    children.add(child.get_owner())

        return list(children)

    def get_parents(self):
        parents = set()
        for input in self.inputs:
            owner = input.get_parent().get_owner()
            parents.add(owner)

        return list(parents)

    def evaluate(self):
        active_ids = [input_node.get_id() for input_node in self.get_inputs() if input_node.is_active()]

        if len(active_ids):
            code_list = WhenBlockRegistry.get_code_list(active_ids)

            self.debug(f'{self.name}: evaluating')

            for code in code_list:
                code(self)

    def get_engine(self):
        if not self.engine:
            for parent in self.get_parents():
                self.engine = parent.get_engine()

        if not self.engine:
            raise(RuntimeError('engine is not set'))

        return self.engine

    def now(self):
        return self.get_engine().now()

    def get_output_value(self, from_outputs=None):
        values = {}

        for output in self.outputs:
            if not from_outputs or output in from_outputs:
                values[output] = output.get_all_value()

        return values

    def __call__(self):
        return self.get_output_value().get(self._output, None)

    def __lshift__(self, other):
        self.debug('result: {}'.format(other))
        self._output = other

    def info(self, msg, *args):
        if self.get_logger().isEnabledFor(logging.INFO):
            self.log(logging.INFO, msg, *args)

    def debug(self, msg, *args):
        if self.get_logger().isEnabledFor(logging.DEBUG):
            self.log(logging.DEBUG, msg, *args)

    def error(self, msg, *args):
        if self.get_logger().isEnabledFor(logging.ERROR):
            self.log(logging.ERROR, msg, *args)

    def critical(self, msg, *args):
        if self.get_logger().isEnabledFor(logging.CRITICAL):
            self.log(logging.CRITICAL, msg, *args)

    def warn(self, msg, *args):
        if self.get_logger().isEnabledFor(logging.WARN):
            self.log(logging.WARN, msg, *args)

    def log(self, level, msg, *args):
        log_msg = msg.format(*args) if len(args) else msg
        now = self.now()
        logical_time = now.strftime('%Y-%m-%d %H:%M:%S.%f') if now is not None else ''
        physical_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
        self.get_logger().log(level, '[{}]-[{}] {}'.format(physical_time, logical_time, log_msg))

    def __str__(self):
        return self.name

    def __repr__(self):
        return str(self)


class Flow(FlowBase, metaclass=MetaFlow):
    pass


class Source(Flow):
    def __init__(self, name):
        super().__init__(name)

    def set_engine(self, engine):
        self.engine = engine

    def peek(self, start_time, end_time):
        pass

    def __lt__(self, other):
        return id(self).__lt__(id(other))

    def close(self):
        pass


class Empty(Source):
    def __init__(self, engine):
        super().__init__('Empty')
        engine.add_source(self)

    def peek(self, start_time, end_time):
        return self, None

    def evaluate(self):
        pass


class Constant(Source):
    def __init__(self, value, engine=None, name=None):
        super().__init__('{}({})'.format(name if name else 'constant', value))
        self.value = value
        self.consumed = False
        if engine is not None:
            engine.add_source(self)

    def peek(self, start_time, end_time):
        if not self.consumed:
            self.consumed = True
            return self, start_time
        else:
            return self, None

    def evaluate(self):
        self << self.value

    def __str__(self):
        return str(self.value)


class Feedback(Source):
    feedback = Input()

    def __init__(self, engine, name='feedback'):
        super().__init__(name)
        self.processed = set()
        self.engine = engine
        self.engine.add_source(self)
        self.is_real_time = isinstance(engine, RealTimeEngine)

    def start(self, start_time, end_time):
        pass

    def __lshift__(self, other):
        self.set_input(other)

    def set_input(self, feedback):
        self.feedback = feedback

    def check(self, start_time, end_time):
        input = self.get_inputs()[0]
        feedback_value = input.get_parent().get_last_value_with_time()
        if feedback_value is not None:
            time = feedback_value[0]
            if time not in self.processed:
                self.processed.add(time)
                return self, time + timedelta(microseconds=self.engine.get_interval())

        return self, None

    def peek(self, start_time, end_time):
        node, time = self.check(start_time, end_time)
        if self.is_real_time and time is not None:
            # FIXME: feedback event should have highest priority
            self.get_engine().get_queue().put((time, node))
        else:
            return node, time

    def evaluate(self):
        self._output = self.get_inputs()[0].get_parent().get_last_value()


class EagerSource(Source):
    def __init__(self, name):
        super().__init__(name)
        self.data = None
        self.index = 0

    def peek(self, start_time, end_time):
        if not self.data:
            self.data = self.get_all_events(start_time, end_time)

        if self.index <= len(self.data) - 1:
            time = self.data[self.index][0]
            if start_time <= time <= end_time:
                return self, time

        return self, None

    def evaluate(self):
        self._output = self.data[self.index][1]
        self.index += 1

    @abstractmethod
    def get_all_events(self, start_time, end_time) -> Sequence[Tuple[datetime.datetime, Any]]:
        pass


class DataSource(EagerSource):
    def __init__(self, engine, data, name='source'):
        super().__init__(name)
        self.cached_data = data
        engine.add_source(self)

    def get_all_events(self, start_time, end_time):
        return self.cached_data


class LazySource(Source):
    def __init__(self, name):
        super().__init__(name)
        self.next = ()

    def peek(self, start_time, end_time):
        if self.next is None:
            return self, None

        if not len(self.next):
            self.next = self.get_next_event(start_time, end_time)

            if self.next is not None:
                return self, self.next[0]
            else:
                return self, None
        else:
            return self, self.next[0]

    def evaluate(self):
        self._output = self.next[1]
        self.next = ()

    def get_next_event(self, start_time, end_time):
        # return (time, value)
        pass


class IntervalTimer(LazySource):
    def __init__(self, engine, interval_in_seconds):
        super().__init__('interval timer')
        self.interval = timedelta(seconds=interval_in_seconds)
        self.current_time = None
        engine.add_source(self)

    def get_next_event(self, start_time, end_time):
        if not self.current_time:
            self.current_time = start_time
            return self.curent_time, None
        else:
            self.current_time += self.interval
            if self.current_time <= end_time:
                return self.current_time, None
            else:
                return None


class FixedTimer(EagerSource):
    def __init__(self, engine, timestamps):
        super().__init__('fixed timer')
        self.timestamps = timestamps
        self.timestamps.sort()
        engine.add_source(self)

    def get_all_events(self, start_time, end_time):
        return [(t, None) for t in self.timestamps if start_time <= t <= end_time]


class Flatten2(Flow):
    input1 = Input()
    input2 = Input()

    def __init__(self, input1, input2, name='flatten', is_fill_empty=False):
        super().__init__(name)
        self.is_fill_empty = is_fill_empty

    @when(input1, input2)
    def f(self):
        result = []
        if self.input1.is_active():
            if isinstance(self.input1(), list):
                result.extend(self.input1())
            else:
                result.append(self.input1())
        elif self.is_fill_empty:
            result.append(None)

        if self.input2.is_active():
            if isinstance(self.input2(), list):
                result.extend(self.input2())
            else:
                result.append(self.input2())
        elif self.is_fill_empty:
            result.append(None)

        if len(result):
            self << result


class Filter(Flow):
    input = Input()

    def __init__(self, input, filter_fun, name='filter'):
        super().__init__(name)
        self.filter_fun = filter_fun

    @when(input)
    def when(self):
        if self.filter_fun(self.now(), self.input()):
            self << self.input()


class Restrict(Flow):
    input = Input()

    def __init__(self, input, restrict_fun, name='restrict'):
        super().__init__(name)
        self.restrict_fun = restrict_fun

    @when(input)
    def when(self):
        restricted = self.restrict_fun(self.input())

        if len(restricted):
            self << restricted


class Fold(Flow):
    input = Input()

    def __init__(self, input, init, fold_fun, name='fold'):
        super().__init__(name)
        self.accum = init
        self.fold_fun = fold_fun

    @when(input)
    def do_fold(self):
        self.accum = self.fold_fun(self.accum, self.input())
        self << self.accum


class EngineListener:
    def engine_started(self, logical_time, physical_time):
        pass

    def engine_finished(self, logical_time, physical_time):
        pass

    def node_finished(self, node_id, logical_time, physical_time):
        pass

    def node_started(self, node_id, logical_time, physical_time):
        pass


class EngineBase:
    def __init__(self, keep_history=False, listener: EngineListener=None, logger_name='flow', logger_level=logging.INFO):
        self.sources = []
        self.current_time = None
        self.interval = 1

        self.keep_history = keep_history
        self.listener = listener

        self.logger_name = logger_name
        self.logger_level = logger_level

        self.graph_root = GraphRoot()
        GraphStack.push(self.graph_root)

        NodeRegistry.clear()

    def get_logger(self):
        return logging.getLogger(self.logger_name)

    def debug(self, msg, *args, **kwargs):
        self.log(logging.DEBUG, msg, args, kwargs)

    def log(self, level, msg, *args):
        if self.get_logger().isEnabledFor(level):
            log_msg = msg.format(*args)
            now = self.now()
            logical_time = now.strftime('%Y-%m-%d %H:%M:%S.%f') if now is not None else ''
            physical_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
            self.get_logger().log(level, '[{}]-[{}] {}'.format(physical_time, logical_time, log_msg))

    def get_interval(self):
        return self.interval

    def visit(self, visit_node, visit_parent_child=None):
        def visit_internal(parent, visited):
            if parent not in visited:
                visit_node(parent)
                visited.add(parent)
                for output in parent.get_outputs():
                    for input_node in output.get_children():
                        child = input_node.get_owner()
                        if visit_parent_child is not None:
                            visit_parent_child(parent, child, input_node)
                        visit_internal(child, visited)

        visited = set()
        for source in self.sources:
            visit_internal(source, visited)

    def now(self):
        return self.current_time

    def add_source(self, source):
        self.sources.append(source)
        source.set_engine(self)

    def remove_source(self, removed_source):
        self.sources = [source for source in self.sources if source != removed_source]

    def sort(self):
        result = []
        queue = []
        queue.extend(self.sources)
        visited = {}

        while len(queue):
            parent = queue.pop()
            result.append(parent)

            for child in parent.get_children(skip_feedback=True):
                if child not in visited:
                    visited[child] = 0

                visited[child] += 1

                if visited[child] == len(child.get_parents()):
                    queue.append(child)

        return result

    def start(self, start_time, end_time, profile=False):
        if self.listener:
            self.listener.engine_started(start_time, datetime.datetime.now())

        if profile:
            import cProfile, pstats, io
            pr = cProfile.Profile()
            pr.enable()
            self._start(start_time, end_time)
            pr.disable()
            s = io.StringIO()
            sort_by = 'tottime'
            ps = pstats.Stats(pr, stream=s).sort_stats(sort_by)
            ps.print_stats(40)
            print(s.getvalue())
        else:
            self._start(start_time, end_time)

        if self.listener:
            self.listener.engine_finished(start_time, datetime.datetime.now())

        for source in self.sources:
            source.close()

    def show_graph(self, file_name='graph', show_cycle=False, show_edge_label=True, same_rank=None):
        if same_rank:
            sr = ' '.join('{}'.format(id(sr)) for sr in same_rank)
            body = ['{' + 'rank=same; {}'.format(sr) + '}']
        else:
            body = None

        graph = gv.Digraph(engine='dot', body=body)

        def add_node(node):
            name = node.get_name()
            graph.node(str(id(node)),
                       color='red' if isinstance(node, Feedback) else 'blue' if isinstance(node, Source)
                       else 'black',
                       label=name if name else str(node.__class__),
                       shape='diamond' if isinstance(node, Feedback) else 'box' if isinstance(node, Source)
                       else 'oval'
                       )

        def add_edge(parent, child, input_node):
            if not show_cycle and isinstance(parent, Feedback):
                return

            name = ''
            if show_edge_label:
                for key in dir(child):
                    try:
                        value = getattr(child, key)
                        if isinstance(value, InputNode) and value.get_id() == input_node.get_id():
                            name = key
                            break
                    except Exception as e:
                        pass

            graph.edge(str(id(parent)), str(id(child)), label=name)

        self.visit(add_node, add_edge)

        graph.render(filename=file_name, view=True)

    def traverse(self, visitor):

        def traverse_graph(parent, visitor):
            for child in parent.get_children():
                visitor(child, parent)
                if isinstance(child, Graph):
                    traverse_graph(child, visitor)

        return traverse_graph(self.graph_root, visitor)

    def create_constant(self, value):
        if isinstance(self, RealTimeEngine):
            return RealTimeConstant(value, self)
        else:
            return Constant(value, self)


class Engine(EngineBase):
    def _start(self, start_time, end_time):
        sorted_list = [n for n in self.sort() if not isinstance(n, Source)]

        queue = []
        queue_cache = {}
        feedbacks = [source for source in self.sources if isinstance(source, Feedback)]

        def is_in_queue(source):
            return source in queue_cache

        while True:
            for feedback in feedbacks:
                if not is_in_queue(feedback):
                    s, t = feedback.peek(start_time, end_time)
                    if t is not None:
                        heapq.heappush(queue, (t, s))
                        queue_cache[s] = True

            for source in self.sources:
                if not isinstance(source, Feedback) and not is_in_queue(source):
                    s, t = source.peek(start_time, end_time)
                    if t is not None:
                        heapq.heappush(queue, (t, s))
                        queue_cache[s] = True

            if not len(queue):
                break

            next_sources = []

            time = queue[0][0]
            while len(queue) and time == queue[0][0]:
                _, source = heapq.heappop(queue)
                del queue_cache[source]
                next_sources.append(source)

            self.current_time = time

            if self.current_time < start_time or self.current_time > end_time:
                break

            self.log(logging.DEBUG, 'start cycle')

            for n in next_sources + sorted_list:
                if self.listener:
                    self.listener.node_started(
                        id(n), self.now(), datetime.datetime.now())

                n.evaluate()

                if self.listener:
                    self.listener.node_finished(
                        id(n), self.now(), datetime.datetime.now())


class RealTimeEngine(EngineBase):
    def __init__(self, keep_history=False):
        super().__init__(keep_history)
        # FIxME: should use priority queue for feedback events
        self.event_queue = Queue()

    def get_queue(self):
        return self.event_queue

    def _start(self, start_time, end_time):
        for source in self.sources:
            source.start(start_time, end_time)

        sorted_nodes = self.sort()
        sorted_list = [n for n in sorted_nodes if not isinstance(n, Source)]
        feedback_list = [source for source in self.sources if isinstance(source, Feedback)]

        wait = start_time - datetime.datetime.now()
        if wait.total_seconds() > 0:
            time.sleep(wait.total_seconds())

        while True:
            try:
                now = datetime.datetime.now()
                max_wait = (end_time - now).total_seconds()

                if max_wait > 0:
                    t, source = self.get_queue().get(block=True, timeout=max_wait)
                    if t > start_time:
                        now = datetime.datetime.now()
                        assert self.current_time is None or self.current_time < now
                        self.current_time = now

                        source.evaluate()

                        for n in sorted_list:
                            n.evaluate()

                        for feedback in feedback_list:
                            feedback.peek(start_time, end_time)
                else:
                    break
            except QueueEmpty as e:
                break
            except RuntimeError as e:
                print(e)
                break


class RealTimeSource(Source):
    def __init__(self, name, engine):
        super().__init__(name)
        self.engine = engine
        self.engine.add_source(self)

    def start(self, start_time, end_time):
        pass

    def close(self):
        pass


class RealTimeEmpty(RealTimeSource):
    def __init__(self, engine):
        super().__init__('RealTimeEmpty', engine)

    def start(self, start_time, end_time):
        pass

    def close(self):
        pass


class RealTimeDataSource(RealTimeSource):
    def __init__(self, name, engine, data):
        super().__init__(name, engine)
        self.data = data
        self.index = 0
        self.start_time = None
        self.end_time = None

    def start(self, start_time, end_time):
        self.start_time = start_time
        self.end_time = end_time

        def schedule():
            for t, v in self.data:
                if self.start_time <= t <= self.end_time:
                    wait = t - datetime.datetime.now()
                    if wait.total_seconds() > 0:
                        time.sleep(wait.total_seconds())
                    self.get_engine().get_queue().put((t, self))
        t = Thread(target=schedule)
        t.start()

    def evaluate(self):
        self._output = self.data[self.index][1]
        self.index += 1

    # FIXME:
    def close(self):
        pass


class RealTimeConstant(RealTimeSource):
    def __init__(self, value, engine):
        super().__init__(f"constant: {value}", engine)
        self.value = value

    def start(self, start_time, end_time):
        self.get_engine().get_queue().put((datetime.datetime.now(), self))

    def evaluate(self):
        self._output = self.value

    # FIXME:
    def close(self):
        pass

    def __str__(self):
        return str(self.value)


class RealTimeFixedTimer(RealTimeSource):
    def __init__(self, engine, timestamps):
        super().__init__('fixed timer', engine)
        self.timestamps = timestamps
        self.timestamps.sort()
        # engine.add_source(self)
        # self.start(None, None)

    def evaluate(self):
        self._output = True
        self.debug(f'{self.name} eval')

    def start(self, start_time, end_time):
        def schedule():
            for t in self.timestamps:
                wait = t - datetime.datetime.now()
                if wait.total_seconds() > 0:
                    self.debug(f'sleep at {datetime.datetime.now()} for {wait.total_seconds()} until {t}')
                    time.sleep(wait.total_seconds())
                    self.debug(f'wake up at {datetime.datetime.now()} instead of {t}')
                self.get_engine().get_queue().put((t, self))

        t = Thread(target=schedule)
        t.start()

    # FIXME:
    def close(self):
        pass


def flatten(inputs, is_fill_empty=False):
    def flatten_internal(input, rest):
        if len(rest):
            f = Flatten2(input, rest[0], 'flatten', is_fill_empty=is_fill_empty)
            return flatten_internal(f, rest[1:])
        else:
            engine = input.get_engine()
            if isinstance(engine, RealTimeEngine):
                return Flatten2(input, RealTimeEmpty(engine), is_fill_empty=False)
            else:
                return Flatten2(input, Empty(engine), is_fill_empty=False)
            # return input

    if not isinstance(inputs, list):
        inputs = [inputs]

    if len(inputs):
        f = flatten_internal(inputs[0], inputs[1:])

        def fill(t, l):
            if len(l) < len(inputs):
                return [None] * (len(inputs) - len(l)) + l
            else:
                return l

        if is_fill_empty:
            return f.map(fill)
        else:
            return f
    else:
        return inputs


class DynamicFlow(FlowBase):
    def __init__(self, *inputs):
        super().__init__('dyn_flow')
        self.params = []

        for input in inputs:
            i = Input()
            if not isinstance(input, FlowBase):
                input = Constant(input, self.get_engine())
            i.__set__(self, input)
            self.params.append(i)

        when(*self.params)(DynamicFlow.handle)

    def handle(self):
        params = [NodeRegistry.get_input(self, id(param)) for param in self.params]
        self.when(*params)

    @abstractmethod
    def when(self, *params):
        pass


class MapN(FlowBase):
    def __init__(self, name, fun, *inputs, timed=False, passive=None, wait_for_all=True, pass_self=False):
        super().__init__(name)
        self.fun = fun
        self.timed = timed
        self.wait_for_all = wait_for_all
        self.pass_self = pass_self

        self.active_params = []
        self.passive_params = []

        params = signature(fun)
        timed_adj = 1 if timed else 0
        param_pos = {i - timed_adj: name for i, name in enumerate(params.parameters.keys())}
        for pos, input in enumerate(inputs):
            i = Input()
            if not (isinstance(input, FlowBase) or isinstance(input, OutputNode)):
                input = Constant(input, self.get_engine())
            i.__set__(self, input)
            param_name = param_pos[pos]
            if passive is None or param_name not in passive:
                self.active_params.append(i)
            else:
                self.passive_params.append(i)

        when(*self.active_params)(MapN.handle)

    def handle(self):
        params = [NodeRegistry.get_input(self, id(param)) for param in self.active_params + self.passive_params]
        has_value = [param.has_value() for param in params]
        if not self.wait_for_all or all(has_value):
            params = [param() for param in params]
            if self.timed:
                params = [self.now()] + params
            if self.pass_self:
                params = [self] + params
            v = self.fun(*params)
            if v is not None:
                self << v


class lift:
    IS_OFF = False

    def __init__(self, name=None, timed=False, passive=None, wait_for_all=True, pass_self=False):
        self.name = name
        self.timed = timed
        self.passive = passive
        self.wait_for_all = wait_for_all
        self.pass_self = pass_self

    def __call__(self, fun):
        if not lift.IS_OFF:
            def map_n(*inputs):
                passive = self.passive if isinstance(self.passive, list) else [self.passive]
                return MapN(self.name, fun, *inputs, timed=self.timed, passive=passive, wait_for_all=self.wait_for_all, pass_self=self.pass_self)
            return map_n
        else:
            return fun


class graph:
    def __init__(self, name):
        self.name = name

    def __call__(self, fun):
        def wrapper(*args, **kwargs):
            parent = GraphStack.peek()
            child = Graph(self.name)
            parent.add_child(child)
            GraphStack.push(child)

            result = fun(*args, *kwargs)

            GraphStack.pop()
            return result

        return wrapper


class SampleMethod(Enum):
    First = 1
    Last = 2


class Sample(Flow):
    input = Input()
    start = Input()

    timer = Timer()

    def __init__(self, input, start, interval: datetime.timedelta, method=SampleMethod.Last, is_auto_start=False):
        super().__init__('sample')
        self.interval = interval
        self.first_cache = None
        self.last_cache = None
        self.started = False
        self.method = method
        self.is_auto_start = is_auto_start

    @when(start)
    def handle(self):
        if not self.started:
            self.timer = self.now()
            self.first_cache = self.input()
            self.last_cache = self.input()
            self.started = True

    @when(timer)
    def handle(self):
        if self.method == SampleMethod.First and self.first_cache is not None:
            self << self.first_cache
        elif self.method == SampleMethod.Last and self.last_cache is not None:
            self << self.last_cache

        self.timer = self.now() + self.interval
        self.first_cache = None
        self.last_cache = None

    @when(input)
    def handle(self):
        if self.first_cache is None:
            self.first_cache = self.input()
        self.last_cache = self.input()
        if self.is_auto_start and not self.started:
            self.timer = self.now() + self.interval
            self.started = True


class FastSample(Flow):
    value = Input()

    def __init__(self, value):
        super().__init__('fast_sample')
        self.last_ts = None

    @when(value)
    def handle(self):
        if self.last_ts is None or self.now().second != self.last_ts.second:
            self.last_ts = self.now()
            self << self.value()


class Shift(Flow):
    input = Input()

    def __init__(self, input, shift):
        super().__init__('shift')
        assert shift > 0
        self.shift = shift
        self.cache = []

    @when(input)
    def handle(self):
        self.cache.append(self.input())
        if len(self.cache) > self.shift:
            self << self.cache[0]
            self.cache = self.cache[1:]


class Wait2(Flow):
    i1 = Input()
    i2 = Input()

    o1 = Output()
    o2 = Output()

    def __init__(self, i1, i2):
        super().__init__('wait2')
        self.wait = True

    @when(i1, i2)
    def handle(self):
        if self.wait and self.i1.has_value() and self.i2.has_value():
            self.o1 = self.i1()
            self.o2 = self.i2()
            self.wait = False
        elif not self.wait:
            if self.i1.is_active():
                self.o1 = self.i1()
            if self.i2.is_active():
                self.o2 = self.i2()


class IgnoreRepeat(Flow):
    i = Input()

    def __init__(self, i, equal_fun=None):
        super().__init__('ignore repeat')
        if equal_fun is None:
            self.equal_fun = lambda a, b: a == b
        else:
            self.equal_fun = equal_fun

        self.last = None

    @when(i)
    def handle(self):
        if not self.equal_fun(self.i(), self.last):
            self << self.i()
            self.last = self.i()


class Snap(Flow):
    value = Input()
    trigger = Input()

    def __init__(self, value, trigger):
        super().__init__('snap')
        self.cache = None
        self.is_snapped = False

    @when(value)
    def handle(self):
        if not self.is_snapped:
            self.cache = self.value()

    @when(trigger)
    def handle(self):
        if not self.is_snapped:
            self.is_snapped = True
            if self.cache is not None:
                self << self.cache


def flow_to_dict(inputs, keep_last=True):
    keys = list(inputs.keys())
    values = list(inputs.values())
    values = flatten(values, is_fill_empty=True)
    return ToDict(values, keys, keep_last)


class ToDict(Flow):
    li = Input()

    def __init__(self, li, keys, keep_last=True):
        super().__init__('merge to dict')
        self.keys = keys
        self.last_cache = {}
        self.keep_last = keep_last

    @when(li)
    def handle(self):
        merged = self.last_cache if self.keep_last else {}

        for i, value in enumerate(self.li()):
            if value is not None:
                merged[self.keys[i]] = value

        if len(merged):
            if self.keep_last:
                self << merged.copy()
            else:
                self << merged


class Rolling(Flow):
    input = Input()

    def __init__(self, input, window: datetime.timedelta):
        super().__init__('rolling')
        self.cache = []
        self.window = window

    @when(input)
    def handle(self):
        self.cache.append((self.now(), self.input()))

        cutoff_index = None
        for i in range(0, len(self.cache)):
            cutoff = self.now() - self.window
            ts, _ = self.cache[i]
            if ts >= cutoff:
                cutoff_index = i
                break
        if cutoff_index is not None:
            self.cache = self.cache[cutoff_index:]

        self << [c for _, c in self.cache]


class Until(Flow):
    value = Input()

    def __init__(self, value, asof):
        super().__init__('until')
        self.asof = asof

    @when(value)
    def handle(self):
        if self.now() <= self.asof:
            self << self.value()


class Start(Flow):
    value = Input()

    def __init__(self, value, asof):
        super().__init__('start')
        self.asof = asof

    @when(value)
    def handle(self):
        if self.now() >= self.asof:
            self << self.value()


class FilterBy(Flow):
    value = Input()
    filter = Input()

    def __init__(self, value, filter, filter_fun, name='filter_by'):
        super().__init__(name)
        self.filter_fun = filter_fun

    @when(value)
    def handle(self):
        if self.filter.has_value():
            if self.filter_fun(self.filter()):
                self << self.value()


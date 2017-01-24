from typing import Sequence, Tuple, Any

import datetime
from datetime import timedelta
import logging
import heapq
from collections import OrderedDict
import graphviz as gv
from inspect import signature

from abc import abstractmethod


class NodeRegistry(object):
    input_registry = {}
    output_registry = {}

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
        return self.last_time and self.last_time == timestamp


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
    registry = {}
    code_cache = {}

    @staticmethod
    def add(class_name, input_id, code):
        if class_name not in WhenBlockRegistry.registry:
            WhenBlockRegistry.registry[class_name] = OrderedDict()

        if code not in WhenBlockRegistry.registry[class_name]:
            WhenBlockRegistry.registry[class_name][code] = []

        WhenBlockRegistry.registry[class_name][code].append(input_id)

    @staticmethod
    def get_code_list(class_name, input_ids):
        input_ids_key = str(input_ids)

        code_list = WhenBlockRegistry.code_cache.get((class_name, input_ids_key))
        if code_list:
            return code_list

        code_list = []
        code_to_ids = WhenBlockRegistry.registry.get(class_name, {})

        for code, ids in code_to_ids.items():
            for input_id in input_ids:
                if input_id in ids and code not in code_list:
                    code_list.append(code)

        WhenBlockRegistry.code_cache[(class_name, input_ids_key)] = code_list
        return code_list


class when:
    def __init__(self, *inputs):
        self.inputs = inputs

    def __call__(self, f):
        tokens = f.__qualname__.split('.')
        class_name = f.__module__ + '.' + '.'.join(tokens[0:-1])

        for input in self.inputs:
            WhenBlockRegistry.add(class_name, id(input), f)


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

        if isinstance(value, Flow):
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
                if not (isinstance(value, OutputNode) or isinstance(value, Flow)):
                    raise TypeError('{} is not type output'.format(value))
                setattr(instance, name, value)

        for index, arg in enumerate(args):
            if index in positions:
                if not (isinstance(arg, OutputNode) or isinstance(arg, Flow)):
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


class Flow(metaclass=MetaFlow):
    _output = Output()

    def __init__(self, name=None):
        self.inputs = []
        self.outputs = []
        self.engine = None
        self.name = name
        self.logger = logging.getLogger(name) if name else logging.getLogger('flow')

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

    def get_class_name(self):
        return self.__module__ + '.' + self.__class__.__qualname__

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
        ids = [input.get_id() for input in self.get_inputs()]

        active_ids = []
        for id in ids:
            input_node = NodeRegistry.get_input(self, id)
            if input_node is not None and input_node.is_active():
                active_ids.append(id)

        if len(active_ids):
            code_list = WhenBlockRegistry.get_code_list(self.get_class_name(), active_ids)

            for code in code_list:
                code(self)

    def get_engine(self):
        if not self.engine:
            for parent in self.get_parents():
                self.engine = parent.get_engine()

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
        result = self.get_output_value().get(self._output, None)
        if result is None:
            raise RuntimeError("no result")
        else:
            return result

    def __getattr__(self, item):
        if self.__class__ != Flow:
            self.warn('created map for {}:{}', self, item)
            return Map(self, lambda value: getattr(value, item), item)

    def __mul__(self, other):
        if not isinstance(other, Flow):
            other = Constant(other)
            self.get_engine().add_source(other)

        return Map2(self, other, lambda input1, input2: input1 * input2, 'mul(*)')

    def __rmul__(self, other):
        if not isinstance(other, Flow):
            other = Constant(other)
            self.get_engine().add_source(other)

        return Map2(other, self, lambda input1, input2: input1 * input2, 'mul(*)')

    def __sub__(self, other):
        if not isinstance(other, Flow):
            other = Constant(other)
            self.get_engine().add_source(other)

        return Map2(self, other, lambda input1, input2: input1 - input2, 'sub(-)')

    def __rsub__(self, other):
        if not isinstance(other, Flow):
            other = Constant(other)
            self.get_engine().add_source(other)

        return Map2(other, self, lambda input1, input2: input1 - input2, 'sub(-)')

    def __add__(self, other):
        if not isinstance(other, Flow):
            other = Constant(other)
            self.get_engine().add_source(other)

        return Map2(self, other, lambda input1, input2: input1 + input2, 'add(+)')

    def __radd__(self, other):
        if not isinstance(other, Flow):
            other = Constant(other)
            self.get_engine().add_source(other)

        return Map2(other, self, lambda input1, input2: input1 + input2, 'add(+)')

    def __pow__(self, other):
        if not isinstance(other, Flow):
            other = Constant(other)
            self.get_engine().add_source(other)

        return Map2(self, other, lambda input1, input2: input1 ** input2, 'pow(**)')

    def __abs__(self):
        return Map(self, lambda input: abs(input), 'abs')

    def __neg__(self):
        return Map(self, lambda input: -input, 'neg(-)')

    def __pos__(self):
        return Map(self, lambda input: +input, 'pos(+)')

    def __truediv__(self, other):
        if not isinstance(other, Flow):
            other = Constant(other)
            self.get_engine().add_source(other)

        return Map2(self, other, lambda input1, input2: input1 / input2, 'div(/)')

    def __rtruediv__(self, other):
        if not isinstance(other, Flow):
            other = Constant(other)
            self.get_engine().add_source(other)

        return Map2(other, self, lambda input1, input2: input1 / input2, 'div(/)')

    def __lshift__(self, other):
        self.debug(other)
        self._output = other

    def __lt__(self, other):
        return self.compare(other, lambda in1, in2: in1 < in2, 'lt(<)')

    def __le__(self, other):
        return self.compare(other, lambda in1, in2: in1 <= in2, 'le(<=)')

    def __gt__(self, other):
        return self.compare(other, lambda in1, in2: in1 > in2, 'gt(>)')

    def __ge__(self, other):
        return self.compare(other, lambda in1, in2: in1 >= in2, 'ge(>=)')

    def compare(self, other, fun, name='compare'):
        if not isinstance(other, Flow):
            other = Constant(other)
            self.get_engine().add_source(other)

        return Map2(self, other, lambda input1, input2: fun(input1, input2), name)

    def filter(self, filter_fun, name='filter'):
        return Filter(self, filter_fun, name)

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

    def flatten(self, inputs):
        if not isinstance(inputs, list):
            input_list = [inputs]
        else:
            input_list = inputs

        return flatten(input_list + [self])

    def probe(self, msg='{}'):
        def l(i):
            self.info(msg.format(i))
            return i

        return Map(self, l, 'probe')

    def info(self, msg, *args):
        if self.logger.isEnabledFor(logging.INFO):
            self.log(logging.INFO, msg, *args)

    def debug(self, msg, *args):
        if self.logger.isEnabledFor(logging.DEBUG):
            self.log(logging.DEBUG, msg, *args)

    def error(self, msg, *args):
        if self.logger.isEnabledFor(logging.ERROR):
            self.log(logging.ERROR, msg, *args)

    def critical(self, msg, *args):
        if self.logger.isEnabledFor(logging.CRITICAL):
            self.log(logging.CRITICAL, msg, *args)

    def warn(self, msg, *args):
        if self.logger.isEnabledFor(logging.WARN):
            self.log(logging.WARN, msg, *args)

    def log(self, level, msg, *args):
        log_msg = msg.format(*args) if len(args) else msg
        now = self.now()
        logical_time = now.strftime('%Y-%m-%d %M:%H:%S.%f') if now is not None else ''
        physical_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
        self.logger.log(level, '[{}]-[{}] {}'.format(physical_time, logical_time, log_msg))

    def map(self, map_fun):
        return MapWithTime(self, map_fun)

    def fold(self, init, accum):
        return Fold(self, init, accum)


class Source(Flow):
    def __init__(self, name):
        super().__init__(name)

    def set_engine(self, engine):
        self.engine = engine

    def peek(self, start_time, end_time):
        pass

    def __lt__(self, other):
        return id(self).__lt__(id(other))


class Empty(Source):
    def __init__(self, engine):
        super().__init__('Empty')
        engine.add_source(self)

    def peek(self, start_time, end_time):
        return self, None

    def evaluate(self):
        pass


class Constant(Source):
    def __init__(self, value, engine=None):
        super().__init__('Constant({})'.format(value))
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


class Feedback(Source):
    feedback = Input()

    def __init__(self, engine, name='feedback'):
        super().__init__(name)
        self.processed = set()
        self.engine = engine
        self.engine.add_source(self)

    def __lshift__(self, other):
        self.set_input(other)

    def set_input(self, feedback):
        self.feedback = feedback

    def peek(self, start_time, end_time):
        input = self.get_inputs()[0]
        feedback_value = input.get_parent().get_last_value_with_time()
        if feedback_value is not None:
            time = feedback_value[0]
            if time not in self.processed:
                self.processed.add(time)
                return self, time + timedelta(microseconds=self.engine.get_interval())

        return self, None

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
        return [(t, None) for t in self.timestamps if start_time <= end_time]


class Map(Flow):
    input = Input()

    def __init__(self, input, map_fun, name='map'):
        super().__init__(name)
        self.map_fun = map_fun

    @when(input)
    def do_map(self):
        self << self.map_fun(self.input())


class MapWithTime(Flow):
    input = Input()

    def __init__(self, input, map_fun, name='map'):
        super().__init__(name)
        self.map_fun = map_fun

    @when(input)
    def do_map(self):
        self << self.map_fun(self.input(), self.now())


class Map2(Flow):
    input1 = Input()
    input2 = Input()

    def __init__(self, input1, input2, map_fun, name='map2'):
        super().__init__(name)
        self.map_fun = map_fun

    @when(input1, input2)
    def do_map(self):
        if self.input1 and self.input2:
            self << self.map_fun(self.input1(), self.input2())

    def __str__(self):
        if self.name is not None:
            return self.name
        else:
            return str(super())


class Map3(Flow):
    input1 = Input()
    input2 = Input()
    input3 = Input()

    def __init__(self, input1, input2, input3, map_fun, name='map3'):
        super().__init__(name)
        self.map_fun = map_fun

    @when(input1, input2, input3)
    def do_map(self):
        if self.input1 and self.input2 and self.input3:
            self << self.map_fun(self.input1(), self.input2(), self.input3())

    def __str__(self):
        if self.name is not None:
            return self.name
        else:
            return str(super())


class Map6(Flow):
    input1 = Input()
    input2 = Input()
    input3 = Input()
    input4 = Input()
    input5 = Input()
    input6 = Input()

    def __init__(self, input1, input2, input3, input4, input5, input6, map_fun, name='map3'):
        super().__init__(name)
        self.map_fun = map_fun

    @when(input1, input2, input3, input4, input5, input6)
    def do_map(self):
        if self.input1 and self.input2 and self.input3:
            self << self.map_fun(self.input1(), self.input2(), self.input3(), self.input4(), self.input5(),
                                 self.input6())

    def __str__(self):
        if self.name is not None:
            return self.name
        else:
            return str(super())


class Map9(Flow):
    input1 = Input()
    input2 = Input()
    input3 = Input()
    input4 = Input()
    input5 = Input()
    input6 = Input()
    input7 = Input()
    input8 = Input()
    input9 = Input()

    def __init__(self, input1, input2, input3, input4, input5, input6, input7, input8, input9, map_fun, name='map3'):
        super().__init__(name)
        self.map_fun = map_fun

    @when(input1, input2, input3, input4, input5, input6, input7, input8, input9)
    def do_map(self):
        if self.input1 and self.input2 and self.input3:
            self << self.map_fun(self.input1(), self.input2(), self.input3(), self.input4(), self.input5(),
                                 self.input6(), self.input7(), self.input8(), self.input9())

    def __str__(self):
        if self.name is not None:
            return self.name
        else:
            return str(super())


class Flatten2(Flow):
    input1 = Input()
    input2 = Input()

    def __init__(self, input1, input2, name='flatten'):
        super().__init__(name)

    @when(input1, input2)
    def f(self):
        result = []
        if self.input1.is_active():
            if isinstance(self.input1(), list):
                result.extend(self.input1())
            else:
                result.apend(self.input1())

            if self.input2.is_active():
                if isinstance(self.input2(), list):
                    result.extend(self.input2())
                else:
                    result.append(self.input2())

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


class Engine:
    def __init__(self, keep_history=False):
        self.sources = []
        self.current_time = None
        self.interval = 1

        self.logger = logging.getLogger('flow')

        self.keep_history = keep_history

    def debug(self, msg, *args, **kwargs):
        self.log(logging.DEBUG, msg, args, kwargs)

    def log(self, level, msg, *args):
        if self.logger.isEnabledFor(level):
            log_msg = msg.format(*args)
            now = self.now()
            logical_time = now.strftime('%Y-%m-%d %H:%M:%S.%f') if now is not None else ''
            physical_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
            self.logger.log(level, '[{}]-[{}] {}'.format(physical_time, logical_time, log_msg))

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

    def _start(self, start_time, end_time):
        sorted_list = [n for n in self.sort() if not isinstance(n, Source)]

        queue = []
        feedbacks = [source for source in self.sources if isinstance(source, Feedback)]

        def is_in_queue(source):
            return any((t, s) for (t, s) in queue if s == source)

        while True:
            for feedback in feedbacks:
                if not is_in_queue(feedback):
                    s, t = feedback.peek(start_time, end_time)
                    if t is not None:
                        heapq.heappush(queue, (t, s))

            for source in self.sources:
                if not isinstance(source, Feedback) and not is_in_queue(source):
                    s, t = source.peek(start_time, end_time)
                    if t is not None:
                        heapq.heappush(queue, (t, s))

            if not len(queue):
                break

            next_sources = []

            time = queue[0][0]
            while len(queue) and time == queue[0][0]:
                _, source = heapq.heappop(queue)
                next_sources.append(source)

            self.current_time = time

            if self.current_time < start_time or self.current_time > end_time:
                break

            for source in next_sources:
                source.evaluate()

            for n in sorted_list:
                n.evaluate()

    def show_graph(self, file_name='graph', show_cycle=False, show_edge_label=True):
        graph = gv.Digraph(engine='dot')

        def add_node(node):
            name = node.get_name()
            graph.node(str(id(node)),
                       color='red' if isinstance(node, Feedback) else 'blue' if isinstance(node, Source)
                       else 'black',
                       label=name if name else node.get_class_name(),
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


def flatten(inputs):
    def flatten_internal(input, rest):
        if len(rest):
            f = Flatten2(input, rest[0], 'flatten')
            return flatten_internal(f, rest[1:])
        else:
            return input

    if not isinstance(inputs, list):
        inputs = [inputs]

    if len(inputs):
        return flatten_internal(inputs[0], inputs[1:])
    else:
        return inputs


class lift:
    def __init__(self, name=None):
        self.name = name

    def __call__(self, fun):
        def map(input):
            return Map(input, fun, fun.__name__ if self.name is None else self.name)

        def map2(input1, input2):
            if not isinstance(input2, Flow):
                input2 = Constant(input2)
                input1.get_engine().add_source(input2)
            return Map2(input1, input2, fun, fun.__name__ if self.name is None else self.name)

        def map3(input1, input2, input3):
            if not isinstance(input2, Flow):
                input2 = Constant(input2)
                input1.get_engine().add_source(input2)
            if not isinstance(input3, Flow):
                input3 = Constant(input3)
                input1.get_engine().add_source(input3)
            return Map3(input1, input2, input3, fun, fun.__name__ if self.name is None else self.name)

        def map6(input1, input2, input3, input4, input5, input6):
            if not isinstance(input2, Flow):
                input2 = Constant(input2)
                input1.get_engine().add_source(input2)
            if not isinstance(input3, Flow):
                input3 = Constant(input3)
                input1.get_engine().add_source(input3)
            if not isinstance(input4, Flow):
                input4 = Constant(input4)
                input1.get_engine().add_source(input4)
            if not isinstance(input5, Flow):
                input5 = Constant(input5)
                input1.get_engine().add_source(input5)
            if not isinstance(input6, Flow):
                input6 = Constant(input6)
                input1.get_engine().add_source(input6)

            return Map6(input1, input2, input3, input4, input5, input6, fun,
                        fun.__name__ if self.name is None else self.name)

        def map9(input1, input2, input3, input4, input5, input6, input7, input8, input9):
            if not isinstance(input2, Flow):
                input2 = Constant(input2)
                input1.get_engine().add_source(input2)
            if not isinstance(input3, Flow):
                input3 = Constant(input3)
                input1.get_engine().add_source(input3)
            if not isinstance(input4, Flow):
                input4 = Constant(input4)
                input1.get_engine().add_source(input4)
            if not isinstance(input5, Flow):
                input5 = Constant(input5)
                input1.get_engine().add_source(input5)
            if not isinstance(input6, Flow):
                input6 = Constant(input6)
                input1.get_engine().add_source(input6)
            if not isinstance(input7, Flow):
                input7 = Constant(input7)
                input1.get_engine().add_source(input7)
            if not isinstance(input8, Flow):
                input8 = Constant(input8)
                input1.get_engine().add_source(input8)
            if not isinstance(input9, Flow):
                input9 = Constant(input9)
                input1.get_engine().add_source(input9)
            return Map9(input1, input2, input3, input4, input5, input6, input7, input8, input9,
                        fun, fun.__name__ if self.name is None else self.name)

        mapped = {1: map, 2: map2, 3: map3, 6: map6, 9: map9}
        sig = signature(fun)
        return mapped[len(sig.parameters.keys())]

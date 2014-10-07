from datetime import datetime
from time import mktime
from uuid import UUID
from decimal import Decimal, localcontext
from random import Random
from string import printable
from sys import float_info


class PythonTypes(Random):
    """
    Subclass of random.Random, implementing methods to generate some basic python types. Methods of this class
    have 'sane' default values to support easy data generation.
    """

    def __init__(self, seed=None):
        Random.__init__(self, seed)
        self.implemented_types_switch = dict(
            date=self.pydate,
            uuid=self.pyuuid,
            bytearray=self.pybytearray,
            boolean=self.pyboolean,
            string=self.pystring,
            int=self.pyint,
            long=self.pylong,
            float=self.pyfloat,
            decimal=self.pydecimal,
            list=self.pylist,
            dict=self.pydict,
            set=self.pyset
        )

        if seed is not None:
            self.seed(seed)

    def generator(self, n, type_to_gen, **args):
        """Generator for all native types.

        :param n: number of items to generate
        :param type_to_gen: type of items to generate
        :param **args: keyword args for the native type generator used
        :return: type generator
        :rtype: generator
        """
        num = 0
        while num < n:
            try:
                yield self.implemented_types_switch[type_to_gen](**args)
                num += 1
            except KeyError:
                raise NotImplementedError(
                    'Generation of type {} not implemented in {}'.format(type_to_gen, self.__class__.__name__))

    def pydate(self, start_date=None, end_date=None, start_timestamp=1388530800, end_timestamp=1420066799):
        """Generates a random timestamp between two dates given either as datetime or timestamp.
        If start_date __and__ end_date are defined, timestamps will be ignored.

        :param optional datetime start_date: start date of time period. default = None
        :param optional datetime end_date: end date of time period. default = None
        :param optional int start_timestamp: start date of time period. default = 1388530800 (2014-01-01 00:00:00)
        :param optional int end_timestamp: end date of time period. default = 1420066799 (2014-12-31 23:59:59)
        :return: random date between start and end
        :rtype: datetime
        """
        if (start_date is not None) & (end_date is not None):
            start_timestamp = int(mktime(start_date.timetuple()))
            end_timestamp = int(mktime(end_date.timetuple()))
        return datetime.fromtimestamp(self.randrange(start_timestamp, end_timestamp+1))

    def pyuuid(self, timestamp=None):
        """Generates a random UUID.

        :param timestamp: timestamp for UUID generation. default = None
        :return: random UUID
        :rtype: uuid
        """

        # code taken & adapted from standard python uuid library (/usr/lib/python2.7/uuid.py)
        if timestamp is not None:
            nanoseconds = int(timestamp * 1e9)
        else:
            nanoseconds = int(mktime(self.pydate().timetuple()) * 1e9)
        # 0x01b21dd213814000 is the number of 100-ns intervals between the
        # UUID epoch 1582-10-15 00:00:00 and the Unix epoch 1970-01-01 00:00:00.
        timestamp = int(nanoseconds // 100) + 0x01b21dd213814000L
        clock_seq = self.randrange(1 << 14L)  # instead of stable storage
        time_low = timestamp & 0xffffffffL
        time_mid = (timestamp >> 32L) & 0xffffL
        time_hi_version = (timestamp >> 48L) & 0x0fffL
        clock_seq_low = clock_seq & 0xffL
        clock_seq_hi_variant = (clock_seq >> 8L) & 0x3fL
        node = self.randrange(1 << 47L)

        return UUID(fields=(time_low, time_mid, time_hi_version, clock_seq_hi_variant, clock_seq_low, node), version=1)

    def pybytearray(self, size=50):
        """Generates a random bytearray.

        :param optional int size: number of bytes in resulting bytearray
        :return: random bytearray
        :rtype: bytearray
        """
        return bytearray([self.randrange(0, 256) for x in xrange(size)])

    def pyboolean(self, chance=.5):
        """Generates a random boolean.

        :param optional float chance: the chance of returning true. default = .5
        :return: random boolean
        :rtype: boolean
        """
        return self.random() <= chance

    def pystring(self, size=10):
        """ Generates a random string.

        :param optional int size: length of the string to generate. default = 10
        :return: random string
        :rtype: string
        """
        lp = len(printable)
        choice = [printable[int(self.random() * lp)] for _ in range(size)]
        res = ''.join(choice)
        return res

    def pyint(self, low=-2147483648, high=2147483647):
        """Generates a random integer.

        :param optional low: lower bound for return value, can be any integer. default = -2147483648
        :param optional high: upper bound for return value, can be any integer. default = 2147483647
        :return: random integer
        :rtype: int or long, depending on generated number & hardware architecture
        """
        return self.randrange(low, high+1)

    def pylong(self, low=-1 * (1 << 63), high=(1 << 63) - 1):
        """ Generates a random long integer.

        :param optional long low: lower bound for return value, can be any integer. default = -9.223.372.036.854.775.808
        :param optional long high: upper bound for return value, can be any integer. default = 9.223.372.036.854.775.807
        :return: random long integer
        :rtype: long
        """
        return self.randrange(low, high+1)

    def pyfloat(self, low=-3.4028235E38, high=3.4028235E38):
        """Generates a random float.

        :param optional low: lower bound for return value. default = -3.4028235E38
        :param optional high: upper bound for return value. default = 3.4028235E38
        :return: random float
        :rtype: float
        """

        return self.uniform(low, high)

    def pydecimal(self,  low=-3.4028235E38, high=3.4028235E38, decimal_places=3):
        """Generates a random decimal.

        :param optional low: lower bound for return value. default = -3.4028235E38
        :param optional high: upper bound for return value. default = 3.4028235E38
        :param optional int decimal_places: number of decimal places. default = 3
        :return: random decimal
        :rtype: decimal
        """

        # The random method returns a float with to many decimal places.
        # As there is no easy way to round a  Decimal, we need to change
        # the Decimal precision locally.
        with localcontext() as ctx:
            ctx.prec = decimal_places
            return +Decimal(self.uniform(low, high))

    def pylist(self, elems=10, elem_type='int', **elem_args):
        """ Generates a list of definable length with items of definable type.

        :param option int elems: length of list. default = 10
        :param optional string elem_type: type of elements in list. default = 'int'
        :param optional dict elem_args: keyword dict of arguments for generation of list elements
        :return: list of length elem_count with items of type elem_type
        :rtype: list
        """
        result = []
        for _ in xrange(elems):
            try:
                result.append(self.implemented_types_switch[elem_type](**elem_args))
            except KeyError:
                raise NotImplementedError(
                    'Generation of type {} not implemented in {}'.format(type, self.__class__.__name__))
        return result

    def pydict(self, elems=10, key_type='int', elem_type='int', **elem_args):
        """ Generates a dict of definable size with keys and items of definable type.

        :param optional int elems: size of dict. default = 10
        :param optional string key_type: type of dict keys. default = 'int'
        :param optional string elem_type: type of elements in dict. default = 'int'
        :param optional dict elem_args: keyword dict of arguments for generation of dict elements
        :return: dict of size elem_count with keys of type key_type and items of type elem_type
        :rtype: dict
        """
        result = dict()
        # Warning: it is not checked whether enough distinct keys
        # can be generated, thus we could end up in an infinite loop!
        while len(result) < elems:
            try:
                result[self.implemented_types_switch[key_type]()] = (
                    self.implemented_types_switch[elem_type](**elem_args))
            except KeyError:
                raise NotImplementedError(
                    'Generation of type {} not implemented in {}'.format(type, self.__class__.__name__))
        return result

    def pyset(self, elems=10, elem_type='int', **elem_args):
        """ Generates a set of definable size with items of definable type.

        :param elems: size of set. default = 10
        :param elem_type: type of elements in set. default = 'int'
        :param elem_args: keyword dict of arguments for generation of set elements
        :return: set of size elem_count with items of type elem_type
        :rtype: set
        """
        result = set()
        # Warning: it is not checked whether enough distinct elements
        # can be generated, thus we could end up in an infinite loop!
        while len(result) < elems:
            try:
                result.add(self.implemented_types_switch[elem_type](**elem_args))
            except KeyError:
                raise NotImplementedError(
                    'Generation of type {} not implemented in {}'.format(type, self.__class__.__name__))
        return result
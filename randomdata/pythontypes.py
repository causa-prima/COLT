from datetime import datetime
from time import mktime
from uuid import UUID
from decimal import Decimal
from random import WichmannHill
from string import printable
from sys import float_info


class PythonTypes(WichmannHill):
    """
    Subclass of random.WichmannHill, implementing methods to generate some basic python types. Methods of this class
    have 'sane' default values to support easy data generation.
    """

    def __init__(self, seed=None):
        WichmannHill.__init__(self, seed)
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

    def getrandbits(self, bits):
        """Generates a unsigned long with maximum value of 2^bits-1.
        This function re-implements the getrandbits-method because the methods of all
        python random number generators seems to rely on some unseedable source.

        :param int bits: number of bits
        :return: random number
        :rtype: long or int, depending on size
        """
        return self.randrange(0, pow(2, bits))

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

    def pyuuid(self):
        """Generates a random UUID.

        :return: random UUID
        :rtype: uuid
        """

        # code taken & adapted from standard python uuid library (/usr/lib/python2.6/uuid.py)

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
        return bytearray([x for x in self.generator(size, 'int', low=0, high=255)])

    def pyboolean(self, chance_of_getting_true=50):
        """Generates a random boolean.

        :param optional int chance_of_getting_true: The chance of returning true, can be a integer 0-100. default = 50
        :return: random boolean
        :rtype: boolean
        """
        return self.randrange(1, 101) <= chance_of_getting_true

    def pystring(self, length=10):
        """ Generates a random string.

        :param optional int length: length of the string to generate. default = 10
        :return: random string
        :rtype: string
        """
        choice = [self.choice(printable) for _ in range(length)]
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

    def pylong(self, low=-1 * ((1 << 52) - 2), high=1 << 53 - 1):
        """ Generates a random long integer.

        :param optional long low: lower bound for return value, can be any integer. default = -4503599627370494
        :param optional long high: upper bound for return value, can be any integer. default = 9007199254740991
        :return: random long integer
        :rtype: long
        """
        # TODO: high and low cannot be bigger/smaller. why these values?
        return self.randrange(low, high+1)

    def pyfloat(self, left_digits=None, right_digits=None, positive=None):
        """ Generates a random float. Unset parameters are randomly generated.

        :param optional int left_digits: number of digits left of comma. default = None
        :param optional int right_digits: number of digits right of comma. default = None
        :param optional boolean positive: should the generated float be positive. default = None
        :return: random float
        :rtype: float
        """

        left_digits = left_digits or self.randrange(1, float_info.dig+1)
        right_digits = right_digits or self.randrange(0, float_info.dig - left_digits +1)
        sign = 1 if positive or self.randrange(0, 2) else -1

        return float("{0}.{1}".format(
            sign * self.randrange(0, pow(10, left_digits)),
            self.randrange(0, pow(10, right_digits))
        ))

    def pydecimal(self, left_digits=None, right_digits=None, positive=None):
        """ Generates a random decimal. Unset parameters are randomly generated.

        :param optional int left_digits: number of digits left of comma. default = None
        :param optional int right_digits: number of digits right of comma. default = None
        :param optional boolean positive: should the generated decimal be positive. default = None
        :return: random float
        :rtype: float
        """
        return Decimal(str(self.pyfloat(left_digits, right_digits, positive)))

    def pylist(self, elem_count=10, elem_type='int', **elem_args):
        """ Generates a list of definable length with items of definable type.

        :param option int elem_count: length of list. default = 10
        :param optional string elem_type: type of elements in list. default = 'int'
        :param optional dict elem_args: keyword dict of arguments for generation of list elements
        :return: list of length elem_count with items of type elem_type
        :rtype: list
        """
        result = []
        for _ in xrange(elem_count):
            try:
                result.append(self.implemented_types_switch[elem_type](**elem_args))
            except KeyError:
                raise NotImplementedError(
                    'Generation of type {} not implemented in {}'.format(type, self.__class__.__name__))
        return result

    def pydict(self, elem_count=10, key_type='int', elem_type='int', **elem_args):
        """ Generates a dict of definable size with keys and items of definable type.

        :param optional int elem_count: size of dict. default = 10
        :param optional string key_type: type of dict keys. default = 'int'
        :param optional string elem_type: type of elements in dict. default = 'int'
        :param optional dict elem_args: keyword dict of arguments for generation of dict elements
        :return: dict of size elem_count with keys of type key_type and items of type elem_type
        :rtype: dict
        """
        result = dict()
        while len(result) < elem_count:
            try:
                result[self.implemented_types_switch[key_type]()] = (
                    self.implemented_types_switch[elem_type](**elem_args))
            except KeyError:
                raise NotImplementedError(
                    'Generation of type {} not implemented in {}'.format(type, self.__class__.__name__))
        return result

    def pyset(self, elem_count=10, elem_type='int', **elem_args):
        """ Generates a set of definable size with items of definable type.

        :param elem_count: size of set. default = 10
        :param elem_type: type of elements in set. default = 'int'
        :param elem_args: keyword dict of arguments for generation of set elements
        :return: set of size elem_count with items of type elem_type
        :rtype: set
        """
        result = set()
        while len(result) < elem_count:
            try:
                result.add(self.implemented_types_switch[elem_type](**elem_args))
            except KeyError:
                raise NotImplementedError(
                    'Generation of type {} not implemented in {}'.format(type, self.__class__.__name__))
        return result
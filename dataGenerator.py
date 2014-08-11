from datetime import datetime
from time import mktime
from uuid import UUID
from decimal import Decimal
from random import WichmannHill
from string import printable
from sys import float_info


class PyRandom(WichmannHill):
    """
    Subclass of random.WichmannHill, implementing methods to generate some basic python types. Methods of this class
    have 'sane' default values to support easy data generation.
    """

    def __init__(self, seed=None):
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
        This function is only needed because the getrandbits method of all
        python random number generators rely on os.urandom, which is not seedable.

        :param int bits: number of bits
        :return: random number
        :rtype: long
        """
        return self.randint(0, pow(2, bits) - 1)

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
        return datetime.fromtimestamp(self.randint(start_timestamp, end_timestamp))

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
        return self.randint(1, 100) <= chance_of_getting_true

    def pystring(self, length=10):
        """ Generates a random string.
        
        :param optional int length: length of the string to generate. default = 10
        :return: random string
        :rtype: string
        """
        chars = printable
        return ''.join(self.choice(chars) for x in range(length))

    def pyint(self, low=-2147483648, high=2147483647):
        """Generates a random integer.
        
        :param optional low: lower bound for return value, can be any integer. default = -2147483648
        :param optional high: upper bound for return value, can be any integer. default = 2147483647
        :return: random integer
        :rtype: int or long, depending on generated number & hardware architecture
        """
        return self.randint(low, high)

    def pylong(self, low=-1 * ((1 << 52) - 2), high=1 << 53 - 1):
        """ Generates a random long integer.
        
        :param optional long low: lower bound for return value, can be any integer. default = -4503599627370494
        :param optional long high: upper bound for return value, can be any integer. default = 9007199254740991
        :return: random long integer
        :rtype: long
        """
        # TODO: high and low cannot be bigger/smaller. why these values?
        return self.randint(low, high)

    def pyfloat(self, left_digits=None, right_digits=None, positive=None):
        """ Generates a random float. Unset parameters are randomly generated.
        
        :param optional int left_digits: number of digits left of comma. default = None
        :param optional int right_digits: number of digits right of comma. default = None
        :param optional boolean positive: should the generated float be positive. default = None
        :return: random float
        :rtype: float
        """

        left_digits = left_digits or self.randint(1, float_info.dig)
        right_digits = right_digits or self.randint(0, float_info.dig - left_digits)
        sign = 1 if positive or self.randint(0, 1) else -1

        return float("{0}.{1}".format(
            sign * self.randint(0, pow(10, left_digits) - 1),
            self.randint(0, pow(10, right_digits) - 1)
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


class CassandraRandom(PyRandom):
    def __init__(self, seed=None):
        PyRandom.__init__(self, seed)
        new_implemented_types_switch = dict(
            ascii=self.pystring,
            bigint=self.pylong,
            blob=self.pybytearray,
            boolean=self.pyboolean,
            counter=self.pyint,
            decimal=self.pydecimal,
            double=self.pyfloat,
            float=self.pyfloat,
            inet=self.ip,
            int=self.pyint,
            text=self.pystring,
            timestamp=self.pydate,
            timeuuid=self.pyuuid,
            uuid=self.pyuuid,
            varchar=self.pystring,
            varint=self.pylong,
            list=self.pylist,
            map=self.pydict,
            set=self.pyset
        )
        self.implemented_types_switch.update(new_implemented_types_switch)

    def ip(self, ip_type='ipv4'):
        """Generates a random IP address.

        :param ip_type: type of IP address, values except 'ipv4' generate IPv6 addresses. default = 'ipv4'
        :return: random IP address
        :rtype: string
        """
        if ip_type == 'ipv4':
            return '.'.join([str(self.choice(xrange(255))) for _ in xrange(4)])
        else:
            return ':'.join([hex(self.choice(xrange(65535)))[2:] for _ in xrange(8)])


rndtest = CassandraRandom(0)

for key in rndtest.implemented_types_switch.keys():
    val = rndtest.implemented_types_switch[key]()
    print key, ' Type of key: ', type(key)
    print val
    if key in ('uuid', 'timeuuid'):
        print datetime.fromtimestamp((val.time - 0x01b21dd213814000L) * 100 / 1e9)
    print
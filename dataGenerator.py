import datetime
from uuid import uuid1 as uuid
from decimal import Decimal
from random import WichmannHill
from string import printable
from sys import float_info
from sys import exc_info

"""
CASSANDRA TYPES

<type> ::= <native-type>
         | <collection-type>
         | <string>       // Used for custom types. The fully-qualified name of a JAVA class

<native-type> ::= ascii
                | bigint
                | blob
                | boolean
                | counter
                | decimal
                | double
                | float
                | inet
                | int
                | text
                | timestamp
                | timeuuid
                | uuid
                | varchar
                | varint

<collection-type> ::= list '<' <native-type> '>'
                    | set  '<' <native-type> '>'
                    | map  '<' <native-type> ',' <native-type> '>'
                    

MAPPED-TO PYTHON TYPES

x str 
x int
x long
x bool
x decimal
x float
list / tuple / generator
dict / OrderedDict
set / frozenset
date / datetime
uuid
buffer / bytearray
"""


class Rndm:
    native_types_switch = dict(
        ascii=pystring,
        bigint=pyint,
        blob=pybytearray,
        boolean=pyboolean,
        counter=pyint,
        decimal=pydecimal,
        double=pyfloat,
        float=pyfloat,
        inet=ip,
        int=pyint,
        text=pystring,
        timestamp=pydate,
        timeuuid=pyuuid,
        uuid=pyuuid,
        varchar=pystring,
        varint=pyint
    )

    collections_type_switch = dict(
        list=pylist,
        map=pydict,
        set=pyset
    )


    def __init__(self):
        self.PRNG = WichmannHill()

    def seed(self, seed):
        """Seeds the random generator with the provided seed.
        
        :param hashable seed: some hashable seed 
        """
        self.PRNG.seed(seed)

    def generator(self, n, type, **args):
        """Generator for all native types.

        :param n: number of items to generate
        :param type: type of items to generate, must be native types
        :param **args: keyword args for the native type generator used
        :return: native type generator
        :rtype: generator
        """
        num = 0
        while num < n:
            try:
                yield self.native_types_switch[type](**args)
                num += 1
            except:
                print exc_info()[0]

    def ip(self, ip_type='ipv4'):
        """Generates a random IP address.

        :param ip_type: type of IP address, values except 'ipv4' generate IPv6 addresses. default = 'ipv4'
        :return: random IP address
        :rtype: string
        """
        if ip_type == 'ipv4':
            return '.'.join([str(self.PRNG.choice(xrange(255))) for _ in xrange(4)])
        else:
            return ':'.join([hex(self.PRNG.choice(xrange(65535)))[2:] for _ in xrange(8)])

    def pydate(self, length=50):
        # TODO: implement
        pass

    def pyuuid(self, length=50):
        # TODO: implement
        pass

    def pybytearray(self, size=50):
        """Generates a random bytearray.

        :param size: number of bytes in resulting bytearray
        :return: random bytearray
        :rtype: bytearray
        """
        return bytearray([x for x in self.generator(size, 'int', high=255)])

    def pyboolean(self, chance_of_getting_true=50):
        """Generates a random boolean.
        
        :param optional int chance_of_getting_true: The chance of returning true, can be a integer 0-100. default = 50
        :return: random boolean
        :rtype: boolean
        """
        return self.PRNG.randint(1, 100) <= chance_of_getting_true

    def pystring(self, length=10):
        """ Generates a random string.
        
        :param optional int length: length of the string to generate. default = 10
        :return: random string
        :rtype: string
        """
        chars = printable
        return ''.join(self.PRNG.choice(chars) for x in range(length))

    def pyint(self, low=0, high=65535):
        """Generates a random integer.
        
        :param optional low: lower bound for return value, can be any integer. default = 0
        :param optional high: upper bound for return value, can be any integer. default = 65535
        :return: random integer
        :rtype: int
        """
        return self.PRNG.randint(low, high)

    def pylong(self, bits=64):
        """ Generates a random long integer.
        
        :param optional int bits: maximum number of bits of generated value. default = 64
        :return: random long integer
        :rtype: long
        """
        return self.PRNG.getrandbits(bits)

    def pyfloat(self, left_digits=None, right_digits=None, positive=None):
        """ Generates a random float. Unset parameters are randomly generated.
        
        :param optional int left_digits: number of digits left of comma. default = None
        :param optional int right_digits: number of digits right of comma. default = None
        :param optional boolean positive: should the generated float be positive. default = None
        :return: random float
        :rtype: float
        """

        left_digits = left_digits or self.PRNG.randint(1, float_info.dig)
        right_digits = right_digits or self.PRNG.randint(0, float_info.dig - left_digits)
        sign = 1 if positive or self.PRNG.randint(0, 1) else -1

        return float("{0}.{1}".format(
            sign * self.PRNG.randint(0, pow(10, left_digits) - 1),
            self.PRNG.randint(0, pow(10, right_digits) - 1)
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
        result = []
        for _ in xrange(elem_count):
            try:
                result.append(self.native_types_switch[elem_type](**elem_args))
            except KeyError:
                print 'generation of Datatype "{}" not supported/implemented.'.format(elem_type)
        return result

    def pydict(self, elem_count=10, key_type='int', elem_type='int', **elem_args):
        result = dict()
        while len(result) < elem_count:
            try:
                result[self.native_types_switch[key_type]()] = (self.native_types_switch[elem_type](**elem_args))
            except KeyError:
                print 'generation of Datatype "{}" not supported/implemented.'.format(elem_type)
        return result

    def pyset(self, elem_count=10, elem_type='int', **elem_args):
        result = set()
        while len(result) < elem_count:
            try:
                result.add(self.native_types_switch[elem_type](**elem_args))
            except KeyError:
                print 'generation of Datatype "{}" not supported/implemented.'.format(elem_type)
        return result


import time
import uuid

uid = uuid.uuid1()
print uid.fields
"""t = Rndm()
t.seed(1234)
start_time = time.time()
for i in xrange(10000000):
    t.pyint()
print time.time() - start_time

wh = WichmannHill()
wh.seed(1234)
start_time = time.time()
for i in xrange(10000000):
    wh.randint(0,65535)
print time.time() - start_time"""


def generateData(datatype, **kwargs):
    print datatype, ':',
    switch = {
        'ascii': generateDataAscii,
        'bigint': fake.random_int,  # parameters: max, min
        'blob': 'blob',
        'boolean': fake.boolean,  # parameters: chance_of_getting_true
        'counter': fake.random_int,  # parameters: max, min
        'decimal': 'decimal',
        'double': 'double',
        'float': 'float',
        'inet': fake.ipv6,
        'int': fake.random_int,  #parameters: max, min
        'text': 'text',
        'timestamp': generateDataTime,  #parameters: start_date, end_date
        'timeuuid': uuid,
        'uuid': uuid,
        'varchar': 'varchar',
        'varint': fake.random_int,  #parameters: max, min
    }
    try:
        data = switch[datatype](**kwargs)
        print type(data), data
    except KeyError:
        print 'generation of Datatype "{}" not implemented.'.format(datatype)


def generateDataAscii(size=20):
    return size


def generateDataTime(start_date='-5y', end_date='now', **kwargs):
    # TODO: timestamps without hh:mm:ss - usefull?
    time = fake.date_time_between(start_date, end_date)
    return time

    # generateData('uuid')
    # generateData('timestamp',start_date=datetime.datetime(2000,01,01,00),end_date=datetime.datetime(2000,01,01,01))
    #generateData('uid')
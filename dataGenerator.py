import datetime
from uuid import uuid1 as uuid

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
from random import WichmannHill
from string import printable
from sys import float_info

class Rndm:
    native-types_switch = dict(
        ascii=pystring,
        bigint=pyint,
        blob=pyblob,
        boolean=pyboolean,
        counter=pyint,
        decimal=pydecimal,
        double=pyfloat,
        float=pyfloat,
        inet=ipv6,
        int=pyint,
        text=pystring,
        timestamp=pydate,
        timeuuid=pyuuid,
        uuid=pyuuid,
        varchar=pystring,
        varint=pyint
    )

    collections-type_switch = dict(
        list=pylist,
        map=pydict,
        set=pyset
    )


    def __init__(self):
        self.generator = WichmannHill()

    def seed(self, seed):
        """Seeds the random generator with the provided seed.
        
        :param hashable seed: some hashable seed 
        """
        self.generator.seed(seed)

    def ipv6(self, length=50):
        # TODO: implement
        pass

    def pydate(self, length=50):
        # TODO: implement
        pass

    def pyuuid(self, length=50):
        # TODO: implement
        pass

    def pybuffer(self, length=50):
        # TODO: implement
        pass

    def pyboolean(self, chance_of_getting_true=50):
        """Generates a random boolean.
        
        :param optional int chance_of_getting_true: The chance of returning true, can be a integer 0-100. default = 50
        :return: random boolean
        :rtype: boolean
        """
        return self.generator.randint(1, 100) <= chance_of_getting_true

    def pystring(self, length=10):
        """ Generates a random string.
        
        :param optional int length: length of the string to generate. default = 10
        :return: random string
        :rtype: string
        """
        chars = printable
        return ''.join(self.generator.choice(chars) for x in range(length))

    def pyint(self, low=0, high=65535):
        """Generates a random integer.
        
        :param optional low: lower bound for return value, can be any integer. default = 0
        :param optional high: upper bound for return value, can be any integer. default = 65535
        :return: random integer
        :rtype: int
        """
        return self.generator.randint(low, high)

    def pylong(self, bits=64):
        """ Generates a random long integer.
        
        :param optional int bits: maximum number of bits of generated value. default = 64
        :return: random long integer
        :rtype: long
        """
        return self.generator.getrandbits(bits)

    def pyfloat(self, left_digits=None, right_digits=None, positive=None):
        """ Generates a random float. Unset parameters are randomly generated.
        
        :param optional int left_digits: number of digits left of comma. default = None
        :param optional int right_digits: number of digits right of comma. default = None
        :param optional boolean positive: should the generated float be positive. default = None
        :return: random float
        :rtype: float
        """

        left_digits = left_digits or self.generator.randint(1, float_info.dig)
        right_digits = right_digits or self.generator.randint(0, float_info.dig - left_digits)
        sign = 1 if positive or self.generator.randint(0, 1) else -1

        return float("{0}.{1}".format(
            sign * self.generator.randint(0, pow(10, left_digits) - 1),
            self.generator.randint(0, pow(10, right_digits) - 1)
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

    def pylist(self, elem_count=10, elem_type='int', elem_args=None):
        pass

    def pydict(self, elem_count=10, key_type='int', elem_type='int', elem_args=None):
        pass

    def pyset(self, elem_count=10, elem_type='int', elem_args=None):
        pass


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
        'boolean': fake.boolean,  #parameters: chance_of_getting_true
        'counter': fake.random_int,  #parameters: max, min
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
    #generateData('timestamp',start_date=datetime.datetime(2000,01,01,00),end_date=datetime.datetime(2000,01,01,01))
    #generateData('uid')
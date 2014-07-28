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
    
    def __init__(self):
        self.generator = WichmannHill()
    
    def seed(self,seed):
        self.generator.seed(seed)
    
    def pyboolean(self, chance_of_getting_true=50):
        return self.generator.randint(1, 100) <= chance_of_getting_true
    
    def pystring(self,length=10):
        """
        Generates a random string.
        @param length: Integer. Length of a password
        @return: String. Random password
        """
        chars = printable
        return ''.join(self.generator.choice(chars) for x in range(length))
    
    def pyint(self,low=0,high=65535):
        return self.generator.randint(low,high)
    
    def pylong(self,bits=64):
        return generator.getrandbits(bits)
    
    def pyfloat(self, left_digits=None, right_digits=None, positive=False):
        left_digits = left_digits or self.int(1, float_info.dig)
        right_digits = right_digits or self.int(0, float_info.dig - left_digits)
        sign = 1 if positive or self.int(0, 1) else -1

        return float("{0}.{1}".format(
            sign * self.int(0,pow(10,left_digits)-1), self.int(0,pow(10,right_digits)-1)
        ))
        
    def pydecimal(self, left_digits=None, right_digits=None, positive=False):
        return Decimal(str(self.pyfloat(left_digits, right_digits, positive)))
    
    def pylist(self, elem_count=10, elem_type='int', elem_args=None):
        pass
    
    def pyset(self, elem_count=10, elem_type='int', elem_args=None):
        pass
    
    def pydict(self, elem_count=10, key_type='int', elem_type='int', elem_args=None):
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
              'bigint' : fake.random_int, #parameters: max, min
              'blob': 'blob',
              'boolean' : fake.boolean, #parameters: chance_of_getting_true
              'counter' : fake.random_int, #parameters: max, min
              'decimal' : 'decimal',
              'double' : 'double',
              'float' : 'float',
              'inet' : fake.ipv6,
              'int' : fake.random_int, #parameters: max, min
              'text' : 'text',
              'timestamp' : generateDataTime, #parameters: start_date, end_date
              'timeuuid' : uuid,
              'uuid' : uuid,
              'varchar' : 'varchar',
              'varint' : fake.random_int, #parameters: max, min
              }
    try: 
        data = switch[datatype](**kwargs)
        print type(data),data
    except KeyError:
        print 'generation of Datatype "{}" not implemented.'.format(datatype) 
    
def generateDataAscii(size=20):
    return size

def generateDataTime(start_date='-5y', end_date='now', **kwargs):
    # TODO: timestamps without hh:mm:ss - usefull?
    time = fake.date_time_between(start_date,end_date)
    return time

#generateData('uuid')
#generateData('timestamp',start_date=datetime.datetime(2000,01,01,00),end_date=datetime.datetime(2000,01,01,01))
#generateData('uid')
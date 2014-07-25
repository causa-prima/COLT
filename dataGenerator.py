import datetime
from uuid import uuid1 as uuid
from faker import Factory

fake = Factory.create()
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

str
int
long
buffer / bytearray
bool
decimal
float
list / tuple / generator
dict / OrderedDict
set / frozenset
date / datetime
uuid
"""

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

import random
wh = random.WichmannHill()
wh.seed(1234)
for i in range(0,10):
    print i, wh.random()
wh.seed(1234)
wh.jumpahead(1)
print wh.random()
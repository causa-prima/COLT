from datetime import datetime
from randomdata.cassandratypes import CassandraTypes


rndtest = CassandraTypes(0)

for key in rndtest.implemented_types_switch.keys():
    val = rndtest.implemented_types_switch[key]()
    print key
    print val
    if key in ('uuid', 'timeuuid'):
        print datetime.fromtimestamp((val.time - 0x01b21dd213814000L) * 100 / 1e9)
    print
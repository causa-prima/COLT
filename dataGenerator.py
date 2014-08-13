from datetime import datetime
import CassandraRandom

rndtest = CassandraRandom(0)

for key in rndtest.implemented_types_switch.keys():
    val = rndtest.implemented_types_switch[key]()
    print key, ' Type of key: ', type(key)
    print val
    if key in ('uuid', 'timeuuid'):
        print datetime.fromtimestamp((val.time - 0x01b21dd213814000L) * 100 / 1e9)
    print
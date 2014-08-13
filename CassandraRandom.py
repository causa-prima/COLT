import PyRandom

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
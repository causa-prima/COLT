from pythontypes import PythonTypes


class CassandraTypes(PythonTypes):
    def __init__(self, seed=None):
        PythonTypes.__init__(self, seed)
        # add special cassandra types to the switch for easy mapping from metadata
        new_methods_switch = dict(
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
        self.methods_switch.update(new_methods_switch)

    def ip(self, ip_type='ipv4'):
        """Generates a random IP address.

        :param ip_type: type of IP address, values except 'ipv4' generate IPv6 addresses. default = 'ipv4'
        :return: random IP address
        :rtype: string
        """
        if ip_type == 'ipv4':
            return '.'.join([str(xrange(255)[int(self.random()*255)]) for _ in xrange(4)])
        else:
            return ':'.join([hex(xrange(65535)[int(self.random()*65535)])[2:] for _ in xrange(8)])

    # TODO: user-defined types introduced in C* 2.1
from sys import argv

from yaml import load

from preparation.cassandrapreparation import CassandraPreparation

switch = {
    'Cassandra' : CassandraPreparation
}

if __name__ == '__main__':
    if len(argv) != 2:
        print 'Wrong argument count.'
        print 'There is just one parameter needed - the path to the location of the configureation file.'
        print "If this path includes spaces, try framing (e.g. '/path with spaces/conf.yaml'"
        exit(1)
    else:
        config_loc = argv[1]
    # throw an exception if config file was not found
    try:
        config = load(open(config_loc, "r"))
    except IOError as e:
        print 'problem loading the configuration file: %s' % e
        exit(1)
    # see what kind of config parser is needed and construct it
    switch[config['config']['database']['type']](config)
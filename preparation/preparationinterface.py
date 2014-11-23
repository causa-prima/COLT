from yaml import dump

from generatorcoordinator import GeneratorCoordinator
from connection.connectioninterface import ConnectionInterface
from randomdata.pythontypes import PythonTypes

class PreparationInterface(object):
    """ Interface to load and process Config objects. Loads and parses
     YAML file from location given on construction on initialization.

    """
    connection_class = ConnectionInterface
    connection = None
    randomdata_class = PythonTypes

    def __init__(self, config=None):
        self.config = config

        try:
            self.connection_args = config['config']['database']['connection arguments']
        except KeyError:
            self.connection_args = {}

        self.process_config()

        gc = GeneratorCoordinator(self.config,
                                  self.randomdata_class,
                                  self.connection_class,
                                  self.connection_args)
        gc.start()

    def delete_old_schema(self):
        ''' Try to delete everything defined in the schemata part of the config
        from the database. If parts of the schema are not present in the
        database DO NOT raise an exception or warning.
        '''
        raise NotImplementedError

    def initialize_schema(self):
        ''' Create everything defined in the schemata part of the config in the
        database.
        '''
        raise NotImplementedError

    def process_config(self):
        """ Method called on initialization to process the parsed YAML file
        further. This method should check for validity of the config statements
        (e.g. presence of stated tables) and additional metadata needed by the
        other program parts. If no further processing is needed, just overwrite
        this method with a simple 'def process_config(self): pass'.

        """
        raise NotImplementedError

    def __repr__(self):
        self.__str__()

    def __str__(self):
        # the YAML package has a nice printing function
        dump(self.config, default_flow_style=False)
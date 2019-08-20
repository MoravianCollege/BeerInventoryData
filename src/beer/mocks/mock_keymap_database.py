
class MockKeyMapDatabase:
    """
    In-memory database for testing.  This class has the same interface as keymap_database.py
    """

    def __init__(self, key_map):
        """
        Initialize with the desired map
        :param key_map: the map
        """
        self.key_map = key_map

    def read_map(self):
        return self.key_map

    def add(self, product, id):
        self.key_map[product] = id



class KeyMap:
    """
    Convert strings to integers for a given table.  Used with products, sizes, and categories.
    
    When a value is requested, if it is not present in the map, a new value is automatically
    added to the map.
    
    This class abstracts away the underlying database.
    """
    
    def __init__(self, keymap_db):
        self.key_map_db = keymap_db
        self.key_map = self.key_map_db.read_map()
        self.next_id = max(self.key_map.values(), default=0) + 1

    def get_value(self, key):
        """
        Get the integer value for a value.  If it is not present in the map, a new
        value will be added.
        :param key: the key to look up
        :return: the integer value
        """
        if key not in self.key_map:
            self.key_map[key] = self.next_id
            self.key_map_db.add(key, self.next_id)
            self.next_id += 1
        return self.key_map[key]


import pandas as pd

class NameMap:
    """
    Convert strings to integers for a names.
    
    When a value is requested, if it is not present in the map, a new value is automatically
    added to the map.
    """
    
    def __init__(self, current_names):
        if len(current_names) > 0:
            self.key_map = dict(zip(current_names['tanczos_name'], current_names['name_id']))
        else:
            self.key_map = {}
        self.new_keys = {}
        self.next_id = max(self.key_map.values(), default=0) + 1

    def get_value(self, name):
        """
        Get the integer value for a value.  If it is not present in the map, a new
        value will be added.
        :param name: the key to look up
        :return: the integer value
        """
        if name not in self.key_map:
            self.key_map[name] = self.next_id
            self.new_keys[name] = self.next_id
            self.next_id += 1
        return self.key_map[name]

    def get_new_names(self):
        data = {'name_id': list(self.new_keys.values()), 'tanczos_name': list(self.new_keys.keys())}
        self.new_keys = {}
        return pd.DataFrame(data=data)

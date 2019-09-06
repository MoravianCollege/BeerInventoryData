
class KeyMap:
    """
    Convert strings to integers for a given table.  Used with products, sizes, and categories.
    
    When a value is requested, if it is not present in the map, a new value is automatically
    added to the map.
    
    This class abstracts away the underlying database.
    """
    
    def __init__(self, conn, table):
        self.conn = conn
        self.table = table
        self.cur = self.conn.cursor()

        self.key_map = {}
        self.new_key_map = {}
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
            self.new_key_map[name] = self.next_id
            self.next_id += 1
        return self.key_map[name]

    def save_new_keys(self):

        for name, value in self.new_key_map.items():
            self.cur.execute('INSERT INTO {} VALUES (%s, %s);'.format(self.table), (name, value))

        self.conn.commit()
        self.new_key_map = {}

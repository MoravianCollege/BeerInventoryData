
from beer.ingest.keymap_database import KeyMapDatabase


class KeyMap:

    def __init__(self, table):
        self.connect(table)
        self.key_map = self.key_map_db.read_map()
        self.next_id = max(self.key_map.values(), default=0) + 1

    def connect(self, table):
        self.key_map_db = KeyMapDatabase(table)

    def get_value(self, product):
        if product not in self.key_map:
            self.key_map[product] = self.next_id
            self.key_map_db.add(product, self.next_id)
            self.next_id += 1
        return self.key_map[product]

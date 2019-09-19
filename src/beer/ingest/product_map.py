class ProductMap:
    """
    Manages the products and creates a unique key for each.

    When a value is requested, if it is not present in the map, a new value is automatically
    added to the map.

    """

    def __init__(self, conn):
        self.conn = conn
        self.cur = self.conn.cursor()

        self.key_map = {}

        self.cur.execute('SELECT * FROM products')

        for row in self.cur.fetchall():
            product_id = row[0]
            values = row[1:]
            self.key_map[values] = product_id

        self.next_id = max(self.key_map.values(), default=0) + 1

    def get_value(self, name_id, container_quantity, quantity_in_case,
                  container_volume, container_type, tanczos_category):
        key = (name_id, container_quantity, quantity_in_case, container_volume, container_type, tanczos_category)

        if key not in self.key_map:
            self.key_map[key] = self.next_id

            self.cur.execute('INSERT INTO products VALUES(%s, %s, %s, %s, %s, %s, %s',
                             (self.next_id, name_id, container_quantity, quantity_in_case,
                              container_volume, container_type, tanczos_category))
            self.conn.commit()

            self.next_id += 1

        return self.key_map[key]

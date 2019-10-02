import pandas as pd


class ProductMap:
    """
    Manages the products and creates a unique key for each.

    When a value is requested, if it is not present in the map, a new value is automatically
    added to the map.

    """

    def __init__(self, current_products):
        if len(current_products) > 0:
            self.key_map = {}
            current_products.apply(lambda row: add_to_dict(self.key_map, (
                row.name_id, row.container_quantity, row.quantity_in_case, row.container_volume,
                row.container_type, row.tanczos_category), row.product_id), axis=1)
        else:
            self.key_map = {}
        self.new_keys = {}
        self.next_id = max(self.key_map.values(), default=0) + 1

    def get_value(self, name_id, container_quantity, quantity_in_case,
                  container_volume, container_type, tanczos_category, timestamp):
        key = (name_id, container_quantity, quantity_in_case, container_volume, container_type, tanczos_category)

        if key not in self.key_map:
            self.key_map[key] = self.next_id
            self.new_keys[key] = (self.next_id, timestamp)
            self.next_id += 1

        return self.key_map[key]

    def get_new_products(self):
        values = pd.DataFrame.from_records(list(self.new_keys.keys()),
                                           columns=['name_id', 'container_quantity', 'quantity_in_case',
                                                    'container_volume', 'container_type', 'tanczos_category'])
        keys = pd.DataFrame.from_records(list(self.new_keys.values()), columns=['product_id', 'timestamp'])
        values['product_id'] = keys['product_id']
        values['timestamp'] = keys['timestamp']
        columns = ['product_id', 'name_id', 'container_quantity', 'quantity_in_case', 'container_volume',
                   'container_type', 'tanczos_category', 'timestamp']
        self.new_keys = {}
        return values[columns]


def add_to_dict(d, k, v):
    d[k] = v
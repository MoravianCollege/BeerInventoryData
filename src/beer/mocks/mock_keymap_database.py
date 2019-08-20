
class MockProductDatabase:

    def __init__(self, products):
        self.products = products

    def read_map(self):
        return self.products

    def add(self, product, id):
        self.products[product] = id


import pandas as pd
import io
from beer.ingest.transactions import compute_transactions, update_inventory


first_timestamp = "2017-11-30 18:43:47"
second_timestamp = "2017-11-30 19:03:35"

class InventoryDataframe:

    inventory_id = 0

    def __init__(self):
        self.str = io.StringIO()

    def add(self, product_id, quantity, price=1.99, timestamp=first_timestamp):
        line = '{},{},{},{},{},{},{},1,{}\n'.format(InventoryDataframe.inventory_id,
                                                                     product_id, product_id, product_id,
                                                                     quantity, price, price, timestamp)
        self.str.write(line)
        InventoryDataframe.inventory_id += 1

    def get_dataframe(self):
        self.str.seek(0)
        return pd.read_csv(self.str, header=None,
                           names=['inventory_id', 'product_id', 'size_id', 'category_id', 'quantity', 'retail',
                                  'case_retail', 'case_pack', 'timestamp'],
                           parse_dates=['timestamp'])


def assert_transaction_exists(result, product_id, transaction_quantity, price=1.99):

    rows = result[result['product_id'] == product_id]
    assert len(rows) == 1
    row = rows.iloc[0]

    assert row['transaction_quantity'] == transaction_quantity
    assert row['retail'] == price
    assert row['case_retail'] == price


def test_empty_results():

    first_idf = InventoryDataframe()
    first_idf.add(product_id=1, quantity=1.0)
    first = first_idf.get_dataframe()

    second_idf = InventoryDataframe()
    second_idf.add(product_id=1, quantity=1.0)
    second = second_idf.get_dataframe()

    result = compute_transactions(first, second)
    assert len(result) == 0


def test_single_item_single_sale():

    first_idf = InventoryDataframe()
    first_idf.add(product_id=1, quantity=5.0)
    first = first_idf.get_dataframe()

    second_idf = InventoryDataframe()
    second_idf.add(product_id=1, quantity=4.0)
    second = second_idf.get_dataframe()

    result = compute_transactions(first, second)
    assert len(result) == 1

    assert_transaction_exists(result, product_id=1, transaction_quantity=1.0)


def test_multiple_item_all_with_sales():

    first_idf = InventoryDataframe()
    first_idf.add(product_id=1, quantity=5.0)
    first_idf.add(product_id=2, quantity=10.0)
    first = first_idf.get_dataframe()

    second_idf = InventoryDataframe()
    second_idf.add(product_id=1, quantity=4.0)
    second_idf.add(product_id=2, quantity=3.0)
    second = second_idf.get_dataframe()

    result = compute_transactions(first, second)
    assert len(result) == 2

    assert_transaction_exists(result, product_id=1, transaction_quantity=1.0)
    assert_transaction_exists(result, product_id=2, transaction_quantity=7.0)


def test_multiple_item_some_with_sales():
    first_idf = InventoryDataframe()
    first_idf.add(product_id=1, quantity=5.0)
    first_idf.add(product_id=2, quantity=10.0)
    first = first_idf.get_dataframe()

    second_idf = InventoryDataframe()
    second_idf.add(product_id=1, quantity=4.0)
    second_idf.add(product_id=2, quantity=10.0)
    second = second_idf.get_dataframe()

    result = compute_transactions(first, second)
    assert len(result) == 1

    assert_transaction_exists(result, product_id=1, transaction_quantity=1.0)


def test_product_missing_from_first():
    first_idf = InventoryDataframe()
    first_idf.add(product_id=1, quantity=5.0)
    first = first_idf.get_dataframe()

    second_idf = InventoryDataframe()
    second_idf.add(product_id=1, quantity=4.0)
    second_idf.add(product_id=2, quantity=10.0)
    second = second_idf.get_dataframe()

    result = compute_transactions(first, second)
    assert len(result) == 2

    assert_transaction_exists(result, 1, 1.0)
    # negative because it is product received
    assert_transaction_exists(result, product_id=2, transaction_quantity=-10.0)


def test_product_missing_from_second():
    first_idf = InventoryDataframe()
    first_idf.add(product_id=1, quantity=5.0)
    first_idf.add(product_id=2, quantity=7.0)
    first = first_idf.get_dataframe()

    second_idf = InventoryDataframe()
    second_idf.add(product_id=1, quantity=4.0)
    second = second_idf.get_dataframe()

    result = compute_transactions(first, second)
    assert len(result) == 1

    assert_transaction_exists(result, product_id=1, transaction_quantity=1.0)


def test_price_change():
    first_idf = InventoryDataframe()
    first_idf.add(product_id=1, quantity=5.0, price=1.99)
    first = first_idf.get_dataframe()

    second_idf = InventoryDataframe()
    second_idf.add(product_id=1, quantity=4.0, price=0.99)
    second = second_idf.get_dataframe()

    result = compute_transactions(first, second)
    assert len(result) == 1

    assert_transaction_exists(result, product_id=1, transaction_quantity=1.0, price=0.99)


def assert_inventory_is(inventory, product_id, quantity, price, timestamp):
    rows = inventory[inventory['product_id'] == product_id]
    assert len(rows) == 1
    row = rows.iloc[0]

    assert row['quantity'] == quantity
    assert row['retail'] == price
    assert row['timestamp'] == pd.to_datetime(timestamp)


def test_new_inventory_replaces_old():

    first_idf = InventoryDataframe()
    first_idf.add(product_id=1, quantity=5.0, price=1.99, timestamp=first_timestamp)
    first = first_idf.get_dataframe()

    second_idf = InventoryDataframe()
    second_idf.add(product_id=1, quantity=5.0, price=1.99, timestamp=second_timestamp)
    second = second_idf.get_dataframe()

    result = update_inventory(first, second)

    assert_inventory_is(result, product_id=1, quantity=5.0, price=1.99, timestamp=second_timestamp)


def test_new_inventory_has_extra():
    first_idf = InventoryDataframe()
    first_idf.add(product_id=1, quantity=5.0, price=1.99, timestamp=first_timestamp)
    first = first_idf.get_dataframe()

    second_idf = InventoryDataframe()
    second_idf.add(product_id=1, quantity=5.0, price=1.99, timestamp=second_timestamp)
    second_idf.add(product_id=2, quantity=3.0, price=2.99, timestamp=second_timestamp)
    second = second_idf.get_dataframe()

    result = update_inventory(first, second)

    assert_inventory_is(result, product_id=1, quantity=5.0, price=1.99, timestamp=second_timestamp)
    assert_inventory_is(result, product_id=2, quantity=3.0, price=2.99, timestamp=second_timestamp)


def test_old_inventory_has_extra():
    first_idf = InventoryDataframe()
    first_idf.add(product_id=1, quantity=5.0, price=1.99, timestamp=first_timestamp)
    first_idf.add(product_id=2, quantity=34.0, price=59.99, timestamp=first_timestamp)
    first = first_idf.get_dataframe()

    second_idf = InventoryDataframe()
    second_idf.add(product_id=1, quantity=5.0, price=1.99, timestamp=second_timestamp)
    second = second_idf.get_dataframe()

    result = update_inventory(first, second)

    assert_inventory_is(result, product_id=1, quantity=5.0, price=1.99, timestamp=second_timestamp)
    assert_inventory_is(result, product_id=2, quantity=34.0, price=59.99, timestamp=first_timestamp)


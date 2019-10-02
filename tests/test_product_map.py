
from beer.ingest.product_map import ProductMap
import pandas as pd

def test_new_item_in_empty_map():
    columns = ['product_id', 'name_id', 'container_quantity', 'quantity_in_case', 'container_volume',
               'container_type', 'tanczos_category', 'timestamp']
    empty_df = pd.DataFrame(columns=columns)
    pm = ProductMap(empty_df)

    assert pm.get_value(1, 1, 1, '24 oz', 'bottle', 'domestic', '2019-09-20 12:59:37') == 1
    df = pm.get_new_products()
    assert len(df) == 1


def test_new_item_in_preexisting_map():
    data = {'product_id': [1], 'name_id': [1], 'container_quantity': [1], 'quantity_in_case': [1],
            'container_volume': ['24 oz'], 'container_type': ['bottle'], 'tanczos_category': ['domestic'],
            'date_added': ['2019-09-20 12:59:37']}
    single_value = pd.DataFrame(data=data)

    pm = ProductMap(single_value)
    assert pm.get_value(2, 2, 2, '12 oz', 'can', 'import', '2019-09-20 12:59:37') == 2

    df = pm.get_new_products()
    assert all(df['product_id'].isin([2]))
    assert all(df['container_volume'].isin(['12 oz']))
    assert len(df) == 1


def test_existing_item():
    data = {'product_id': [3], 'name_id': [3], 'container_quantity': [3], 'quantity_in_case': [3],
            'container_volume': ['16 oz'], 'container_type': ['can'], 'tanczos_category': ['cider'],
            'date_added': ['2019-09-20 12:59:37']}
    single_value = pd.DataFrame(data=data)

    pm = ProductMap(single_value)
    assert pm.get_value(3, 3, 3, '16 oz', 'can', 'cider', '2019-09-20 12:59:37') == 3

    df = pm.get_new_products()
    assert len(df) == 0


def test_existing_item_with_different_timestamp():
    data = {'product_id': [3], 'name_id': [3], 'container_quantity': [3], 'quantity_in_case': [3],
            'container_volume': ['16 oz'], 'container_type': ['can'], 'tanczos_category': ['cider'],
            'date_added': ['2019-01-20 12:59:37']}
    single_value = pd.DataFrame(data=data)

    pm = ProductMap(single_value)
    assert pm.get_value(3, 3, 3, '16 oz', 'can', 'cider', '2019-09-20 12:59:37') == 3

    df = pm.get_new_products()
    assert len(df) == 0

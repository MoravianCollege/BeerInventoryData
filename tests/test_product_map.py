
from beer.ingest.product_map import ProductMap
from beer.mocks.mock_database import MockConnection


def test_new_item_in_empty_map():
    empty_db = MockConnection([])
    pm = ProductMap(empty_db)
    assert pm.get_value(1, 1, 1, '24 oz', 'bottle', 'domestic') == 1
    commands = empty_db.the_cursor.commands
    # query and insert in commands
    assert len(commands) == 2


def test_new_item_in_preexisting_map():
    single_value = MockConnection([(1, 1, 1, 1, '24 oz', 'bottle', 'domestic')])
    pm = ProductMap(single_value)
    assert pm.get_value(2, 2, 2, '12 oz', 'can', 'import') == 2
    commands = single_value.the_cursor.commands
    # query and insert
    assert len(commands) == 2


def test_existing_item():
    single_value = MockConnection([(3, 3, 3, 3, '16 oz', 'can', 'cider')])
    pm = ProductMap(single_value)
    assert pm.get_value(3, 3, 3, '16 oz', 'can', 'cider') == 3
    commands = single_value.the_cursor.commands
    # query only
    assert len(commands) == 1
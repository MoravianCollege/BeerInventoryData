
from beer.ingest.transform import Transform
from beer.mocks.mock_keymap_database import MockKeyMapDatabase
from beer.ingest.keymap import KeyMap


def test_empty_database_tables():
    products = KeyMap(MockKeyMapDatabase({}))
    sizes = KeyMap(MockKeyMapDatabase({}))
    categories = KeyMap(MockKeyMapDatabase({}))
    t = Transform(products, sizes, categories)

    values = t.transform(["name","1/2 KEG","CIDER","0.00","153.5000","153.5000","1","2017-11-30 18:43:47.476167"])

    assert values == [1, 1, 1, '0.00', '153.5000', '153.5000', '1', '2017-11-30 18:43:47']


def test_values_in_database():
    products = KeyMap(MockKeyMapDatabase({'name': 42}))
    sizes = KeyMap(MockKeyMapDatabase({'1/2 KEG': 15}))
    categories = KeyMap(MockKeyMapDatabase({'CIDER': 23}))
    t = Transform(products, sizes, categories)

    values = t.transform(["name","1/2 KEG","CIDER","0.00","153.5000","153.5000","1","2017-11-30 18:43:47.476167"])

    assert values == [42, 15, 23, '0.00', '153.5000', '153.5000', '1', '2017-11-30 18:43:47']


def test_values_not_in_database():
    products = KeyMap(MockKeyMapDatabase({'other': 42}))
    sizes = KeyMap(MockKeyMapDatabase({'cans': 15}))
    categories = KeyMap(MockKeyMapDatabase({'beer': 23}))
    t = Transform(products, sizes, categories)

    values = t.transform(["name","1/2 KEG","CIDER","0.00","153.5000","153.5000","1","2017-11-30 18:43:47.476167"])

    # all values 1 bigger than largest value in DB
    assert values == [43, 16, 24, '0.00', '153.5000', '153.5000', '1', '2017-11-30 18:43:47']

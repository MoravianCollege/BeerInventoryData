
from beer.ingest.keymap import KeyMap
from beer.mocks.mock_keymap_database import MockProductDatabase
from pytest import fixture


def empty_connect(self, table):
    self.key_map_db = MockProductDatabase({})


def test_get_new_products(mocker):
    mocker.patch.object(KeyMap, 'connect', empty_connect)
    p = KeyMap('products')
    assert p.get_value('beer') == 1
    assert p.get_value('grog') == 2


def populated_connect(self, table):
    self.key_map_db = MockProductDatabase({'grog': 1, 'beer': 2})


def test_prexisting_products(mocker):
    mocker.patch.object(KeyMap, 'connect', populated_connect)
    p = KeyMap('products')
    # grog is 1...
    assert p.get_value('beer') == 2
    assert p.get_value('food') == 3

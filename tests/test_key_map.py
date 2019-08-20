
from beer.ingest.keymap import KeyMap
from beer.mocks.mock_keymap_database import MockProductDatabase
from pytest import fixture


def empty_connect(self, table):
    self.key_map_db = MockProductDatabase({})


@fixture
def empty_db(mocker):
    mocker.patch.object(KeyMap, 'connect', empty_connect)


def test_get_new_products(empty_db):
    p = KeyMap('products')
    assert p.get_value('beer') == 1
    assert p.get_value('grog') == 2


def populated_connect(self, table):
    self.key_map_db = MockProductDatabase({'grog': 1, 'beer': 2})


@fixture
def populated_db(mocker):
    mocker.patch.object(KeyMap, 'connect', populated_connect)


def test_prexisting_products(populated_db):
    # setup...
    p = KeyMap('products')
    # something else is 1...
    assert p.get_value('beer') == 2

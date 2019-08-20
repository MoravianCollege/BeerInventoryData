
from beer.ingest.keymap import KeyMap
from beer.mocks.mock_keymap_database import MockKeyMapDatabase


def test_get_new_products():
    p = KeyMap(MockKeyMapDatabase({}))
    assert p.get_value('beer') == 1
    assert p.get_value('grog') == 2


def test_prexisting_products():
    p = KeyMap(MockKeyMapDatabase({'grog': 1, 'beer': 2}))
    # grog is 1, beer is 2
    assert p.get_value('beer') == 2
    assert p.get_value('food') == 3

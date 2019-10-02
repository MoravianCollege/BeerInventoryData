
from beer.ingest.keymap import KeyMap
from beer.mocks.mock_database import MockConnection


def test_get_value_then_save():
    mock_conn = MockConnection()

    p = KeyMap(mock_conn, 'names')
    assert p.get_value('beer') == 1
    assert p.get_value('grog') == 2

    p.save_new_keys()

    mock_cursor = mock_conn.cursor()

    assert len(mock_cursor.commands) == 2


def test_get_value_and_save_intertwined():
    mock_conn = MockConnection()

    p = KeyMap(mock_conn, 'names')
    assert p.get_value('beer') == 1

    p.save_new_keys()

    mock_cursor = mock_conn.cursor()

    assert len(mock_cursor.commands) == 1
    assert 'beer' in mock_cursor.commands[0]

    assert p.get_value('grog') == 2

    p.save_new_keys()
    assert len(mock_cursor.commands) == 2
    assert 'grog' in mock_cursor.commands[-1]
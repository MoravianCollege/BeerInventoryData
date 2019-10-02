
from beer.ingest.name_map import NameMap
import pandas as pd


def test_get_value_then_get_table():
    empty_df = pd.DataFrame(columns=['name_id', 'tanczos_name'])

    p = NameMap(empty_df)
    assert p.get_value('beer') == 1
    assert p.get_value('grog') == 2

    df = p.get_new_names()
    assert len(df) == 2

    assert all(df['tanczos_name'].isin(['beer', 'grog']))
    assert all(df['name_id'].isin([1, 2]))


def test_get_value_and_save_intertwined():
    empty_df = pd.DataFrame(columns=['name_id', 'tanczos_name'])

    p = NameMap(empty_df)
    assert p.get_value('beer') == 1

    df = p.get_new_names()
    assert len(df) == 1

    assert all(df['tanczos_name'].isin(['beer']))
    assert all(df['name_id'].isin([1]))

    assert p.get_value('grog') == 2

    df = p.get_new_names()
    assert len(df) == 1

    assert all(df['tanczos_name'].isin(['grog']))
    assert all(df['name_id'].isin([2]))


def test_existing_values_respected():
    pre_existing = pd.DataFrame(data={'name_id': [1, 2], 'tanczos_name': ['beer', 'grog']})
    p = NameMap(pre_existing)
    assert p.get_value('beer') == 1
    assert p.get_value('grog') == 2

    assert p.get_value('cider') == 3

    df = p.get_new_names()
    assert len(df) == 1

    assert all(df['tanczos_name'].isin(['cider']))
    assert all(df['name_id'].isin([3]))

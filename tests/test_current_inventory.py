

from beer.ingest.current_csv_inventory import CurrentCSVInventory
import io
import pandas as pd

header = '"name","size","category","quantity","retail","case_retail","case_pack","timestamp"'
bells = '"BELLS BEST BROWN","6/12 OZ BTL","CRAFT","4.00","11.8500","40.9900","4","2018-11-21 17:20:25.438956"'
bells_dup = '"BELLS BEST BROWN","6/12 OZ BTL","CRAFT","32.00","11.8500","40.9900","4","2018-11-21 17:20:25.438956"'
bud = '"BUDWEISER","12/12 OZ BTL","DOMESTIC","32.00","11.4900","21.4900","2","2018-11-21 17:20:25.438956"'
bells2 = '"BELLS BEST BROWN","6/12 OZ BTL","CRAFT","4.00","11.8500","40.9900","4","2018-11-21 17:40:25.438956"'
bud2 = '"BUDWEISER","12/12 OZ BTL","DOMESTIC","32.00","11.4900","21.4900","2","2018-11-21 17:40:25.438956"'


def test_first_call_returns_parameter():
    columns = ['name', 'size', 'category', 'quantity', 'retail', 'case_retail', 'case_pack', 'timestamp']
    empty_df = pd.DataFrame(columns=columns)
    mf = CurrentCSVInventory(empty_df)
    data = io.StringIO(header + '\n' + bells + '\n' + bud)
    df = pd.read_csv(data)

    result = mf.update_inventory(df)

    assert len(result) == 2
    assert all(result['name'].isin(['BELLS BEST BROWN', 'BUDWEISER']))


def test_missing_row_added():
    columns = ['name', 'size', 'category', 'quantity', 'retail', 'case_retail', 'case_pack', 'timestamp']
    empty_df = pd.DataFrame(columns=columns)
    mf = CurrentCSVInventory(empty_df)
    data = io.StringIO(header + '\n' + bells + '\n' + bud)
    df = pd.read_csv(data)
    mf.update_inventory(df)

    data2 = io.StringIO(header + '\n' + bud2)
    df2 = pd.read_csv(data2)

    result = mf.update_inventory(df2)

    assert len(result) == 2
    assert all(result['name'].isin(['BELLS BEST BROWN', 'BUDWEISER']))
    # timestamp is updated to the 2nd timestamp in bells2
    assert not any(result['timestamp'].isin(['2018-11-21 17:20:25.438956']))


def test_new_row_preserved():
    columns = ['name', 'size', 'category', 'quantity', 'retail', 'case_retail', 'case_pack', 'timestamp']
    empty_df = pd.DataFrame(columns=columns)
    mf = CurrentCSVInventory(empty_df)
    data = io.StringIO(header + '\n' + bells)
    df = pd.read_csv(data)
    mf.update_inventory(df)

    data2 = io.StringIO(header + '\n' + bells2 + '\n' + bud2)
    df2 = pd.read_csv(data2)
    result = mf.update_inventory(df2)

    assert len(result) == 2
    assert all(result['name'].isin(['BELLS BEST BROWN', 'BUDWEISER']))


def test_pre_existing_value_used():

    data = io.StringIO(header + '\n' + bells + '\n' + bud)
    pre_existing_df = pd.read_csv(data)

    mf = CurrentCSVInventory(pre_existing_df)

    data2 = io.StringIO(header + '\n' + bud2)
    df2 = pd.read_csv(data2)

    result = mf.update_inventory(df2)

    assert len(result) == 2
    assert all(result['name'].isin(['BELLS BEST BROWN', 'BUDWEISER']))
    # timestamp is updated to the 2nd timestamp in bells2
    assert not any(result['timestamp'].isin(['2018-11-21 17:20:25.438956']))

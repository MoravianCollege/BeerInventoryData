

from beer.mocks.mock_database_transactions import MockDBTransactions
from beer.ingest.ingest import Ingest
import io
import pandas as pd


header = '"Name","size","Category","Quantity_Available","Retail","Case Retail","Case Pack","timestamp"'
bells = '"BELLS BEST BROWN","6/12 OZ BTL","CRAFT","4.00","11.8500","40.9900","4","2018-11-21 17:20:25.438956"'
bells_rep = '1,1,1,4,2018-11-21 17:20:25,4.0,11.85,40.99\n'
bud = '"BUDWEISER","12/12 OZ BTL","DOMESTIC","32.00","11.4900","21.4900","2","2018-11-21 17:20:25.438956"'
bud_rep = '2,2,2,2,2018-11-21 17:20:25,32.0,11.49,21.49\n'
zero = '"BUD","6/12 OZ BTL","DOMESTIC","0.00","11.8500","40.9900","4","2018-11-21 17:20:25.438956"'
incomplete = '"BELLS BEST BROWN","6/12 OZ BTL","CRAFT","4.00","11.85"'
part_dup1 = '"DUP","6/12 OZ BTL","CRAFT","40.00","12.9900","40.9900","4","2018-11-21 17:20:25.438956"'
part_dup2 = '"DUP","6/12 OZ BTL","CRAFT","2.00","11.8500","45.9900","4","2018-11-21 17:20:25.438956"'


def run_convert(*records):
    str_data = header + '\n' + '\n'.join(records)
    csv_data = io.StringIO(str_data)
    data = pd.read_csv(csv_data)
    mock_transactions_db = MockDBTransactions([[], [], []])
    i = Ingest(mock_transactions_db)
    return i.convert(data)


def assert_command_run(cursor, *command_substrings):
    for command_substring in command_substrings:
        assert any(command_substring in command for command in cursor.commands)


def test_single_row():
    results = run_convert(bells)

    assert len(results) == 1
    assert 1 in results.product_id.values
    assert 4.0 in results.quantity.values
    assert 11.85 in results.retail.values
    assert 40.99 in results.case_retail.values
    assert '2018-11-21 17:20:25' in results.timestamp.values


def test_na_rows_dropped():
    results = run_convert(bells, incomplete)

    assert len(results) == 1
    assert 1 in results.product_id.values
    assert 4.0 in results.quantity.values
    assert 11.85 in results.retail.values
    assert 40.99 in results.case_retail.values
    assert '2018-11-21 17:20:25' in results.timestamp.values


def test_full_duplicates_removed():
    # file containing the same beer twice
    results = run_convert(bells, bells)

    assert len(results) == 1
    assert 1 in results.product_id.values
    assert 4.0 in results.quantity.values
    assert 11.85 in results.retail.values
    assert 40.99 in results.case_retail.values
    assert '2018-11-21 17:20:25' in results.timestamp.values


def test_partial_duplicates_aggregated():
    results = run_convert(part_dup1, part_dup2)

    assert len(results) == 1
    assert 1 in results.product_id.values
    # sum of quantities
    assert 42.0 in results.quantity.values
    # max from dup1
    assert 12.99 in results.retail.values
    # max from dup2
    assert 45.99 in results.case_retail.values
    assert '2018-11-21 17:20:25' in results.timestamp.values


def test_zero_quantity_removed():
    results = run_convert(bells, zero)

    assert len(results) == 1
    assert 1 in results.product_id.values
    assert 4.0 in results.quantity.values
    assert 11.85 in results.retail.values
    assert 40.99 in results.case_retail.values
    assert '2018-11-21 17:20:25' in results.timestamp.values



def assert_command_run(cursor, command_substring):
    assert any(command_substring in command for command in cursor.commands)


def test_single_line_ingest():
    mock_transactions_db = MockDBTransactions([[], [], []])
    i = Ingest(mock_transactions_db)
    data = io.StringIO(header + '\n' + bells)
    i.ingest(data)

    db_trans_cmds = mock_transactions_db.commands
    assert len(db_trans_cmds) == 5
    (table0, data0, columns0) = db_trans_cmds[0]
    (table1, data1, columns1) = db_trans_cmds[1]
    (table2, data2, columns2) = db_trans_cmds[2]
    (table3, data3, columns3) = db_trans_cmds[3]
    (table4, data4, columns4) = db_trans_cmds[4]
    assert table0 == 'timestamps'
    assert len(data0) == 1
    assert table1 == 'names'
    assert len(data1) == 1
    assert table2 == 'products'
    assert len(data2) == 1
    assert table3 == 'current_inventory'
    assert len(data3) == 1
    assert table4 == 'inventory'
    assert len(data4) == 1


def test_double_line_ingest():
    mock_transactions_db = MockDBTransactions([[], [], []])
    i = Ingest(mock_transactions_db)
    data = io.StringIO(header + '\n' + bells + '\n' + bud)
    i.ingest(data)

    db_trans_cmds = mock_transactions_db.commands
    assert len(db_trans_cmds) == 5
    (table0, data0, columns0) = db_trans_cmds[0]
    (table1, data1, columns1) = db_trans_cmds[1]
    (table2, data2, columns2) = db_trans_cmds[2]
    (table3, data3, columns3) = db_trans_cmds[3]
    (table4, data4, columns4) = db_trans_cmds[4]
    assert table0 == 'timestamps'
    assert len(data0) == 1
    assert table1 == 'names'
    assert len(data1) == 2
    assert table2 == 'products'
    assert len(data2) == 2
    assert table3 == 'current_inventory'
    assert len(data3) == 2
    assert table4 == 'inventory'
    assert len(data4) == 2


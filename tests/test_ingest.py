

from beer.ingest.keymap import KeyMap
from beer.ingest.timestamp_database import TimestampDatabase
from beer.mocks.mock_database import MockConnection
from beer.ingest.ingest import Ingest
import io


header = '"Name","size","Category","Quantity_Available","Retail","Case Retail","Case Pack","timestamp"'
bells = '"BELLS BEST BROWN","6/12 OZ BTL","CRAFT","4.00","11.8500","40.9900","4","2018-11-21 17:20:25.438956"'
bells_rep = '1,1,1,4,2018-11-21 17:20:25,4.0,11.85,40.99\n'

bud = '"BUDWEISER","12/12 OZ BTL","DOMESTIC","32.00","11.4900","21.4900","2","2018-11-21 17:20:25.438956"'
bud_rep = '2,2,2,2,2018-11-21 17:20:25,32.0,11.49,21.49\n'


def assert_command_run(cursor, command_substring):
    assert any(command_substring in command for command in cursor.commands)


def test_single_line_ingest():
    products_conn = MockConnection()
    products = KeyMap(products_conn, 'products')
    sizes_conn = MockConnection()
    sizes = KeyMap(sizes_conn, 'sizes')
    categories_conn = MockConnection()
    categories = KeyMap(categories_conn, 'categories')
    timestamps_conn = MockConnection()
    timestamps = TimestampDatabase(timestamps_conn)

    ingest_conn = MockConnection()
    i = Ingest(ingest_conn, products, sizes, categories, timestamps)
    data = io.StringIO(header + '\n' + bells)
    i.ingest(data)

    assert_command_run(ingest_conn.cursor(), bells_rep)
    assert_command_run(products_conn.cursor(), 'INSERT INTO products')
    assert_command_run(products_conn.cursor(), 'BELLS')
    assert_command_run(sizes_conn.cursor(), 'INSERT INTO sizes')
    assert_command_run(sizes_conn.cursor(), '6/12 OZ BTL')
    assert_command_run(categories_conn.cursor(), 'INSERT INTO categories')
    assert_command_run(categories_conn.cursor(), 'CRAFT')
    assert_command_run(timestamps_conn.cursor(), 'INSERT INTO timestamps')
    assert_command_run(timestamps_conn.cursor(), '2018-11-21 17:20:25')


def test_double_line_ingest():
    conn = MockConnection()
    products = KeyMap(conn, 'products')
    sizes = KeyMap(conn, 'sizes')
    categories = KeyMap(conn, 'categories')
    timestamps = TimestampDatabase(conn)

    i = Ingest(conn, products, sizes, categories, timestamps)
    data = io.StringIO(header + '\n' + bells + '\n' + bud)
    i.ingest(data)

    mock_cursor = conn.cursor()
    assert_command_run(mock_cursor, bells_rep)
    assert_command_run(mock_cursor, bud_rep)


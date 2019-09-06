

from beer.ingest.keymap import KeyMap
from beer.mocks.mock_keymap_database import MockKeyMapDatabase
from beer.mocks.mock_database import MockConnection
from beer.mocks.mock_timestamp_db import MockTimestampDatbase
from beer.ingest.ingest import Ingest
import io


header = '"Name","size","Category","Quantity_Available","Retail","Case Retail","Case Pack","timestamp"'
bells = '"BELLS BEST BROWN","6/12 OZ BTL","CRAFT","4.00","11.8500","40.9900","4","2018-11-21 17:20:25.438956"'
bells_rep = '1,1,1,4,2018-11-21 17:20:25,4.0,11.85,40.99\n'

bud = '"BUDWEISER","12/12 OZ BTL","DOMESTIC","32.00","11.4900","21.4900","2","2018-11-21 17:20:25.438956"'
bud_rep = '2,2,2,2,2018-11-21 17:20:25,32.0,11.49,21.49\n'


def test_single_line_ingest():
    products = KeyMap(MockKeyMapDatabase({}))
    sizes = KeyMap(MockKeyMapDatabase({}))
    categories = KeyMap(MockKeyMapDatabase({}))

    conn = MockConnection()

    timestamps = MockTimestampDatbase()

    i = Ingest(conn, products, sizes, categories, timestamps)

    data = io.StringIO(header + '\n' + bells)

    i.ingest(data)

    mock_cursor = conn.cursor()
    assert mock_cursor.output.getvalue() == bells_rep


def test_double_line_ingest():
    products = KeyMap(MockKeyMapDatabase({}))
    sizes = KeyMap(MockKeyMapDatabase({}))
    categories = KeyMap(MockKeyMapDatabase({}))

    conn = MockConnection()

    timestamps = MockTimestampDatbase()

    i = Ingest(conn, products, sizes, categories, timestamps)

    data = io.StringIO(header + '\n' + bells + '\n' + bud)

    i.ingest(data)

    mock_cursor = conn.cursor()
    assert mock_cursor.output.getvalue() == bells_rep + bud_rep

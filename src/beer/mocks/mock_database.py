
class MockConnection:

    def __init__(self):
        self.the_cursor = MockCursor()

    def cursor(self):
        return self.the_cursor

    def commit(self):
        pass


class MockCursor:

    def __init__(self):
        self.output = None
        self.table = None
        self.sep = None
        self.columns = None

    def copy_from(self, output, table, sep, columns):
        self.output = output
        self.table = table
        self.sep = sep
        self.columns = columns


class MockConnection:

    def __init__(self):
        self.the_cursor = MockCursor()

    def cursor(self):
        return self.the_cursor

    def commit(self):
        pass


class MockCursor:

    def __init__(self):
        self.commands = []

    def copy_from(self, output, table, sep, columns):
        cmd = '{} {} {} {}'.format(output.getvalue(), table, sep, columns)
        self.commands.append(cmd)

    def execute(self, query, *args):
        self.commands.append(query + repr(args))

    def reset(self):
        self.commands = []
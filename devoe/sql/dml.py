from sqlalchemy.sql.schema import Table
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.selectable import Selectable
from sqlalchemy.sql.expression import Executable, ClauseElement, FunctionElement

# CLASSES FOR COMPILATIONS.

class merge(Executable, ClauseElement):
    def __init__(self, table):
        if isinstance(table, Table) is True:
            self.table = table
        else:
            raise AttributeError(
                'attribute <table> must be <sqlalchemy.sql.schema.Table>')
        pass

    def using(self, source, *keys):
        if isinstance(source, (Table, Selectable)) is True:
            self.source = source
        self.keys = keys
        return self

    def update(self, **values):
        self.updvals = values
        return self

    def insert(self, **values):
        self.insvals = values
        return self

# COMPILATIONS

@compiles(merge)
def compile(element, compiler, **kwargs):
    pass

@compiles(merge, 'oracle', 'postgresql')
def compile(element, compiler, **kwargs):
    statement = []
    statement.append(f'MERGE INTO {element.table}')
    if isinstance(element.source, Table) is True:
        statement.append(f'USING {element.source}')
    else:
        statement.append(f'USING (\n{element.source}\n) {element.source.name}')

    keys = '\nAND '.join([str(key) for key in element.keys])
    statement.append(f'ON (\n{keys}\n)')

    statement.append('WHEN MATCHED THEN UPDATE')
    updates = []
    for key, value in element.updvals.items():
        if len(updates) == 0:
            updates.append(f'SET {key} = {value}')
        else:
            updates.append(f'{key} = {value}')
    updates = ', '.join(updates)
    statement.append(updates)

    if hasattr(element, 'insvals') is True and element.insvals is not None:
        statement.append('WHEN NOT MATCHED THEN INSERT')
        columns = ', '.join([str(key) for key in element.insvals.keys()])
        columns = f'({columns})'
        values = ', '.join([str(value) for value in element.insvals.values()])
        values = f'VALUES ({values})'
        statement.extend([columns, values])

    statement = '\n'.join(statement)
    return statement

# Regular methods

def parallel(stmt, dop='auto'):
    hint = f'/*+ PARALLEL({dop}) */'
    return stmt.prefix_with(hint, dialect='oracle')

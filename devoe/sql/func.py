from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.expression import FunctionElement

class trim(FunctionElement):
    name = 'trim'
    pass

@compiles(trim)
def visit_trim(element, compiler, **kwargs):
    if len(element.clauses) == 1:
        return 'TRIM(%s) AS %s' % (
            compiler.process(element.clauses, **kwargs),
            list(element.clauses)[0].name)

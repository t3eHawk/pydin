import sqlalchemy as sql

class alter(sql.schema.DDLElement):
    def __init__(self, object, action, *args, **kwargs):
        self.object = object
        self.action = action
        self.args = args
        self.kwargs = kwargs
        pass

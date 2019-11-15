import pepperoni
import sqlalchemy as sql


def make_log(obj=None):
    if isinstance(obj, pepperoni.Logger) is True:
        log = obj
    else:
        log = pepperoni.logger()
    return log

def make_table(db, name, schema=None):
    table = sql.Table(
        name, db.metadata,
        sql.Column(
            'operation_id', sql.Integer,
            sql.Sequence(f'{name}_seq'), primary_key=True,
            comment='Unique ETL operation ID'),
        sql.Column(
            'operation_date', sql.Date,
            comment='ETL operation time'),
        sql.Column(
            'initiator', sql.String(50),
            comment='OS user who initiated ETL operation'),
        sql.Column(
            'job', sql.Integer,
            comment='Job ID that performed the ETL operation'),
        sql.Column(
            'start_date', sql.Date,
            comment='Time when ETL execution started'),
        sql.Column(
            'end_date', sql.Date,
            comment='Time when ETL execution finished'),
        sql.Column(
            'status', sql.String(1),
            comment='P - in progress, E - error, D - done'),
        sql.Column(
            'records_read', sql.Integer,
            comment='Total number of all records read'),
        sql.Column(
            'records_written', sql.Integer,
            comment='Total number of all records written'),
        sql.Column(
            'records_updated', sql.Integer,
            comment='Total number of all records updated'),
        sql.Column(
            'errors', sql.Integer,
            comment='Total number of errros'),
        sql.Column(
            'files_read', sql.Integer,
            comment='Total number of files read'),
        sql.Column(
            'files_written', sql.Integer,
            comment='Total number of files written'),
        sql.Column(
            'bytes_read', sql.Integer,
            comment='Total volume of bytes read'),
        sql.Column(
            'bytes_written', sql.Integer,
            comment='Total volume of bytes written'),
        sql.Column(
            'log_date', sql.Date,
            comment='Last logging date for that ETL operation'),
        sql.Column(
            'log_record', sql.Text,
            comment='Logging records of current ETL operation'),
        oracle_compress=True if db.vendor == 'oracle' else None,
        schema=schema)
    table.create(db.engine, checkfirst=True)
    return table

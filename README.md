# PyDin

[![version](https://img.shields.io/pypi/v/pydin)](https://pypi.org/project/pydin/)
[![release](https://img.shields.io/github/v/release/t3eHawk/pydin?include_prereleases)](https://github.com/t3eHawk/pydin/)
[![release date](https://img.shields.io/github/release-date-pre/t3eHawk/pydin)](https://github.com/t3eHawk/pydin/releases/)
[![last commit](https://img.shields.io/github/last-commit/t3eHawk/pydin)](https://github.com/t3eHawk/pydin/commits/)
[![downloads](https://img.shields.io/pypi/dm/pydin)](https://pypi.org/project/pydin/)
[![python](https://img.shields.io/pypi/pyversions/pydin)](https://pypi.org/project/pydin/)
[![license](https://img.shields.io/pypi/l/pydin)](https://mit-license.org)

**PyDin** is a Python-based ETL framework that allows you to create, automate
and maintain data processes right out of the box.

This was originally a simple script on a small server to run a dozen of jobs
in the context of a bigger project.
But as the project grew, the script evolved and eventually became
something else.

Thus, **PyDin** is a framework from Data Engineers to Data Engineers
that follows some best practices and philosophy.
It was designed to work in a real production environment with thousands of
daily jobs.

More information can be found in the official [documentation](https://pydin.readthedocs.io/en/latest/#).

## Installation
Just install using [pip](https://pip.pypa.io/en/stable/getting-started/):
```bash
pip install pydin
```

## Interface
The product provides several types of native interfaces: Framework, CLI and API.

Most of the time we use _Framework_ as developers, _CLI_ as administrators and
_API_ as advanced business users and third parties.

Using the CLI as the most convenient interface assumes the creation of a
special _manager.py_ file with the following code in it:
```python
import pydin


manager = pydin.Manager()
```

When the file is saved, you can set an alias for it, like in the example below,
and run any command it can take.
```bash
# Set an alias to run the file.
alias pydin='python ~/pydin/manager.py'

# Get the list of available commands.
pydin help
```

Visit the [page](https://pydin.readthedocs.io/en/latest/#) to learn more about
the CLI and other interfaces.

## Framework
Data ETL processes are represented as _Pipelines_ that can be built using
Framework _Models_.

Here are some code snippets on how these _Pipelines_ can be implemented:

```python
import pydin as pd


# Load data from one database to another.
a = pd.Table(source_name='chinook', table_name='customers', chunk_size=1000)
b = pd.Table(source_name='dwh', table_name='customers')
pipeline = pd.Pipeline(a, b)
pipeline.run()

# Load data by SQL SELECT from one database to another.
select = 'select * from customers where company is not null'
a = pd.Select(source_name='chinook', text=select, chunk_size=1000)
b = pd.Table(source_name='dwh', table_name='companies')
pipeline = pd.Pipeline(a, b)
pipeline.run()

# Load data by SQL INSERT, using SELECT taken from the file.
path = '~/documents/reports/customers.sql'
n = pd.Insert(source_name='dwh', table_name='customers_report', path=path)
pipeline = pd.Pipeline(n)
pipeline.run()

# Load data by SQL INSERT, using a date filter and adding a key field.
path = '~/documents/history/invoices.sql'
n = pd.Insert(source_name='dwh', table_name='invoices_history',
              select=path, date_field='invoice_date', days_back=1,
              key_field=pd.process_id)
pipeline = pd.Pipeline(n)
pipeline.run()

# Load data from database into CSV file with pre-clearance.
select = 'select * from customers where company is not null'
a = pd.Select(source_name='chinook', text=select, chunk_size=10000)
b = pd.CSV(path='customers.csv', delimiter='\t', cleanup=True)
pipeline = pd.Pipeline(a, b)
pipeline.run()

# Load data from database into JSON and XML files at the same time.
a = pd.Table(source_name='dwh', table_name='customers', chunk_size=10000)
b = pd.JSON(path='customers.json', cleanup=True)
c = pd.XML(path='customers.xml', cleanup=True)
pipeline = pd.Pipeline(a, b, c)
pipeline.run()

# Load data from CSV file into database.
a = pd.CSV(path='customers.csv', delimiter='\t', chunk_size=1000)
b = pd.Table(source_name='dwh', table_name='customers')
pipeline = pd.Pipeline(a, b)
pipeline.run()

# Move files from GZ archives into directory.
n = pd.FileManager(path='/archives', mask='.*gz',
                   action='move', dest='/data', unzip=True)
pipeline = pd.Pipeline(n)
pipeline.run()

# Copy files from one directory to another with GZ archiving.
n = pd.FileManager(path='/data', mask='.*csv',
                   action='copy', dest='/backup', zip=True)
pipeline = pd.Pipeline(n)
pipeline.run()
```
Every _Model_ stands for a specific data object.
See the [documentation](https://pydin.readthedocs.io/en/latest/#) for a
description and advanced use of _Models_.

## Automation
The product has a built-in _Scheduler_ for the processes automation.

Install the _Scheduler_ using the command below and following the instructions
step by step:
```bash
pydin install
```

After that, create and configure the _Job_:
```bash
pydin create job
```
Files of created objects are placed in the same location as _manager.py_.

The most important file is _script.py_ from the directory _jobs/n_,
where _n_ is the unique _Job_ ID. In this file you can develop your _Pipeline_
or write any Python code you want to schedule. We usually create one _Job_
per _Pipeline_.

When you are ready, just start the _Scheduler_.
Your ETL process is automated now.
```bash
pydin start scheduler
```

### Database
For advanced **PyDin** features, such as process scheduling and maintenance,
a database is required.
Two database types are supported at the moment: **Oracle** and **SQLite**.
Functionality is not a question of using one of them, you can get all the
features using a simple SQLite database.

## Transformations
Data transformation is an important part of most ETL processes. You very often need to change something in your data streams.
That's why in **PyDin** we have a special _Mapper_ that allows you to do any transformations with data records.
Feel free to use any Python constructs or built-in _Framework_
[tools](https://pydin.readthedocs.io/en/latest/#).

For instance, the following script explains how to change field names and convert a _string_ to a _datetime_ as required by the target table.
```python
import pydin as pd
import datetime as dt


def transform(input_data):
    output_data = {}
    output_data['invoice_id'] = input_data['InvoiceId']
    output_data['customer_id'] = input_data['CustomerId']
    output_data['invoice_date'] = dt.datetime.fromisoformat(input_data['InvoiceDate'])
    output_data['billing_address'] = input_data['BillingAddress']
    output_data['billing_city'] = input_data['BillingCity']
    output_data['billing_state'] = input_data['BillingState']
    output_data['billing_country'] = input_data['BillingCountry']
    output_data['billing_postal_code'] = input_data['BillingPostalCode']
    output_data['total'] = input_data['Total']
    return output_data


a = pd.Table(source_name='chinook', table_name='invoices', chunk_size=1000)
b = pd.Mapper(func=transform)
c = pd.Table(source_name='dwh', table_name='invoices')
pipeline = pd.Pipeline(a, b, c)
pipeline.run()
```

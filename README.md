devoe
=====

Devoe is a Python ETL framework allowing you to create pipelines and customize
your ETL process as you wish.

Features
--------

Installation
------------
```
$ pip install devoe
```

How to use
----------
Below is the example of simple ETL task to extract data from one table to
another.
```python
import devoe as de


select = de.Select(database='billing', file='select.sql')
table = de.Table(database='dwh', schema='billing', name='invoices')
pipeline = de.Pipeline(select, table)
pipeline.run()
```

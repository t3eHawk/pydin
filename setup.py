"""Setup."""

import pydin
import setuptools


name = pydin.__name__
version = pydin.__version__
author = pydin.__author__
author_email = pydin.__email__
description = pydin.__doc__.splitlines()[0]
long_description = open('README.md', 'r').read()
long_description_content_type = 'text/markdown'
license = pydin.__license__
url = 'https://github.com/t3eHawk/pydin'
install_requires = ['pepperoni', 'sqlalchemy', 'cx_Oracle', 'paramiko',
                    'flask', 'flask_httpauth', 'waitress',
                    'sqlparse', 'gitpython']
packages = setuptools.find_packages()
classifiers = ['Programming Language :: Python :: 3',
               'License :: OSI Approved :: MIT License',
               'Operating System :: OS Independent']


setuptools.setup(name=name,
                 version=version,
                 author=author,
                 author_email=author_email,
                 description=description,
                 long_description=long_description,
                 long_description_content_type=long_description_content_type,
                 license=license,
                 url=url,
                 install_requires=install_requires,
                 packages=packages,
                 classifiers=classifiers)

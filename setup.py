"""Setup."""

import devoe
import setuptools


name = devoe.__name__
version = devoe.__version__
author = devoe.__author__
author_email = devoe.__email__
description = devoe.__doc__.splitlines()[0]
long_description = open('README.md', 'r').read()
long_description_content_type = 'text/markdown'
license = devoe.__license__
url = 'https://github.com/t3eHawk/devoe'
install_requires = ['pepperoni', 'sqlalchemy', 'cx_Oracle', 'paramiko',
                    'flask', 'flask_httpauth', 'waitress', 'gitpython']
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

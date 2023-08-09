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
python_requires = '>=3.7'
install_requires = open('requirements.txt').read().splitlines()
packages = setuptools.find_packages()
package_data = {'pydin': ['demo/*', 'samples/*']}
classifiers = ['Programming Language :: Python :: 3.7',
               'Programming Language :: Python :: 3.11',
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
                 python_requires=python_requires,
                 install_requires=install_requires,
                 packages=packages,
                 package_data=package_data,
                 classifiers=classifiers)

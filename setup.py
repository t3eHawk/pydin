import devoe
import setuptools


with open('README.md', 'r') as fh:
    long_description = fh.read()

install_requires = []

author = devoe.__author__
email = devoe.__email__
version = devoe.__version__
description = devoe.__doc__
license = devoe.__license__

setuptools.setup(
    name='devoe',
    version=version,
    author=author,
    author_email=email,
    description=description,
    long_description=long_description,
    long_description_content_type='text/markdown',
    license=license,
    url='https://github.com/t3eHawk/devoe',
    install_requires=install_requires,
    packages=setuptools.find_packages(),
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
)

from setuptools import setup

VERSION = '0.0.1'

setup(name='snowpark_extensions',
      version=VERSION,
      description='A set of helpers to extend snowpark functionality',
      url='http://github.com/MobilizeNet/snowpark-extensions-py',
      author='mauricio.rojas',
      requires=['snowflake-snowpark-python','pandas','shortuuid'],
      author_email='mauricio.rojas@mobilize.net',
      packages=['snowpark_extensions'],
      zip_safe=False)
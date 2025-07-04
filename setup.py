from setuptools import setup

# read the contents of your README file
from pathlib import Path
this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()

VERSION = '0.0.47'

setup(name='snowpark_extensions',
      version=VERSION,
      description='A set of helpers to extend snowpark functionality. NOTE: most of this extensions are already available in the official snowpark python package',
      long_description=long_description,
      long_description_content_type='text/markdown',
      url='https://github.com/Snowflake-Labs/snowpark-extensions-py',
      author='mauricio.rojas',
      install_requires=['snowflake-snowpark-python[pandas]>=1.32.0','jinja2'],
      author_email='mauricio.rojas@snowflake.com',
      packages=['snowpark_extensions'],
      zip_safe=False)

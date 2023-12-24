import pathlib

from setuptools import setup, find_packages

HERE = pathlib.Path(__file__).parent

VERSION = '1.0.0'
PACKAGE_NAME = 'bmi_calculator_challenge'
AUTHOR = 'Ram'
AUTHOR_EMAIL = 'c.ramprasad273@email.com'
URL = 'https://github.com/Ramprasad273/code-20211201-ramprasad'

LICENSE = 'MIT License'
DESCRIPTION = 'Application to calculate BMI and get health risk info'
LONG_DESCRIPTION = (HERE / "README.md").read_text()
LONG_DESC_TYPE = "text/markdown"

INSTALL_REQUIRES = [
    'pyspark~=3.0.1',
    'atomicwrites==1.4.0',
    'attrs==21.2.0',
    'colorama==0.4.4',
    'coverage==6.2',
    'iniconfig==1.1.1',
    'packaging==21.3',
    'pluggy==1.0.0',
    'py==1.11.0',
    'pyparsing==3.0.6',
    'pyspark==3.0.1',
    'pytest==6.2.5',
    'toml==0.10.2',
]

setup(name=PACKAGE_NAME,
      version=VERSION,
      description=DESCRIPTION,
      long_description=LONG_DESCRIPTION,
      long_description_content_type=LONG_DESC_TYPE,
      author=AUTHOR,
      license=LICENSE,
      author_email=AUTHOR_EMAIL,
      url=URL,
      install_requires=INSTALL_REQUIRES,
      packages=find_packages()
      )

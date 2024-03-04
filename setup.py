# pip install -e . (note the dot, it stands for "current directory")
from setuptools import setup, find_packages

setup(
    name="fill_db_quotes",
    version="0.1.0",
    # url="https://github.com/e2e4Konstantin/Create_Fill_DB_quotes.git",

    author="Konstantin Kazak",
    author_email='e2e4suchok@gmail.com',

    description='Creating a strict database Normative by SQLite3',
    # long_description=open('README.md').read(),

    packages=find_packages()
)

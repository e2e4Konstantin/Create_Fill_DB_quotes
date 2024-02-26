# pip install -e . (note the dot, it stands for "current directory")
from setuptools import setup, find_packages

setup(
    name="fill db quotes",
    version="0.1.0",
    description='Creating a strict database Normative by SQLite3',
    author_email='e2e4suchok@gmail.com',
    packages=find_packages()
)

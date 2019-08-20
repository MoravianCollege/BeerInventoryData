
from setuptools import setup, find_packages

setup(
    name="beer",
    packages=find_packages('src'),
    package_dir={'': 'src'}
)

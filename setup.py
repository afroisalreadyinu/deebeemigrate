from deebeemigrate import __version__
from setuptools import setup

setup(
    name='deebeemigrate',
    version=__version__,
    description='Safely and automatically migrate database schemas',
    author='Dan Bravender',
    author_email='dan.bravender@gmail.com',
    entry_points={'console_scripts': ['deebeemigrate = deebeemigrate.core:main']},
    packages=['deebeemigrate'],
)

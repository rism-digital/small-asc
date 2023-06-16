from distutils.core import setup

setup(
    name='small-asc',
    version='0.12.1',
    packages=['small_asc'],
    package_data={
        'small-asc': ['py.typed'],
    },
    url='https://github.com/rism-digital/small-asc',
    license='MIT',
    author='Andrew Hankinson',
    author_email='andrew.hankinson@rism.digital',
    description='A small Solr client'
)

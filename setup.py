from setuptools import setup

setup(
    name="small-asc",
    version="0.23.0",
    packages=["small_asc"],
    package_data={
        "small-asc": ["py.typed"],
    },
    url="https://github.com/rism-digital/small-asc",
    license="MIT",
    author="Andrew Hankinson",
    author_email="andrew.hankinson@rism.digital",
    description="A small asynchronous Solr client",
)

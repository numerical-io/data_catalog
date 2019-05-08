from setuptools import setup, find_packages

exec(open('data_catalog/version.py').read())

setup(
    name = "data_catalog",
    version = __version__,
    packages = find_packages(),

    install_requires = [
        'pandas>=0.19', 'dask>=0.2.0', 's3fs>=0.2.0', 'pytz>=2011k'],

    url = "https://github.com/numerical-io/data_catalog",
    maintainer = "Harold Sneessens",
    maintainer_email = "harold.sneessens@numerical.io",
    description = "A catalog to store, define, and access datasets",
    keywords = ""
)

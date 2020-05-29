import pytest

from data_catalog.core import DataCatalog, CsvDataset


class TestDataCatalog:

    def should_be_created_from_list(self):
        a = CsvDataset('raw_a', relpath='raw/something.csv')
        b = CsvDataset('raw_b', relpath='raw/something_else.csv')
        catalog = DataCatalog.from_list([a, b])

        assert 'raw_a' in catalog
        assert 'raw_b' in catalog
        assert len(catalog) == 2

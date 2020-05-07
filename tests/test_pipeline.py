from pathlib import Path
import time

import pytest
import pandas as pd
import numpy as np

from data_catalog.core import DataCatalog, FileDataset, CsvDataset, \
    ParquetDataset, Pipeline
from data_catalog.file_systems import LocalFileSystem
from data_catalog.core.datasets import AbstractDataset
from data_catalog.core.pipeline import _create_task


@pytest.fixture
def default_catalog(tmpdir):
    # Create test datasets
    FileDataset.file_system = LocalFileSystem(tmpdir)
    a = CsvDataset(
        'raw_a', relpath='raw/dataset_a.csv', read_kwargs={'index_col': 0})
    df = pd.DataFrame({'a': [1, 2]})
    a.write(df)

    b = CsvDataset(
        'raw_b', relpath='raw/dataset_b.csv', read_kwargs={'index_col': 0})
    df = pd.DataFrame({'a': [4, 8]})
    b.write(df)

    # Create dependent datasets
    def sum_of_datasets(x, y):
        return x + y
    c = CsvDataset(
        'bse_c', create=sum_of_datasets, parents=['raw_a', 'raw_b'],
        read_kwargs={'index_col': 0})
    d = CsvDataset(
        'bse_d', create=sum_of_datasets, parents=['raw_a', 'bse_c'],
        read_kwargs={'index_col': 0})
    e = CsvDataset(
        'ftr_e', create=sum_of_datasets, parents=['bse_c', 'bse_d'],
        read_kwargs={'index_col': 0})

    # Create catalog
    data_catalog = DataCatalog()
    for dataset in [a, b, c, d, e]:
        data_catalog.add(dataset)
    return data_catalog


class TestPipeline:
    def should_create_file_from_parents(self, default_catalog):

        # Run pipeline and check generated dataset
        Pipeline(default_catalog).run()
        df = default_catalog['bse_c'].read()
        assert np.allclose(df.values.flatten(), [5, 10])

    def should_update_only_changes(self, default_catalog):
        # Run the pipeline
        Pipeline(default_catalog).run()

        # Modify a dataset
        df = default_catalog['bse_d'].read()
        default_catalog['bse_d'].write(2*df)

        # Check file update times before and after the pipeline run
        time_c = default_catalog['bse_c'].last_update_time()
        time_e = default_catalog['ftr_e'].last_update_time()
        Pipeline(default_catalog).run()
        new_time_c = default_catalog['bse_c'].last_update_time()
        new_time_e = default_catalog['ftr_e'].last_update_time()

        assert new_time_c == time_c
        assert new_time_e >= time_e


class TestCreateTask:
    def should_resolve_dependencies(self):
        a = CsvDataset('raw_a', relpath='raw/something.csv')
        b = CsvDataset('raw_b', relpath='raw/something_else.csv')
        c = CsvDataset(
            'bse_c', create=lambda x, y: x+y, parents=['raw_a', 'raw_b'])
        catalog = DataCatalog.from_list([a, b, c])

        task, parents = _create_task(catalog, 'raw_a')
        assert parents is None

        task, parents = _create_task(catalog, 'bse_c')
        assert parents == ['raw_a', 'raw_b']

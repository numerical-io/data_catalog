from pathlib import Path

import pytest
import pandas as pd

import data_catalog.core.datasets as ds
from data_catalog.core.datasets import AbstractDataset
from data_catalog.core import FileDataset, CsvDataset
from data_catalog.file_systems import LocalFileSystem


class TestFindMandatoryArguments:

    def should_detect_only_mandatory_arguments(self):
        def my_func(a, b, c=3):
            pass
        arguments = ds._find_mandatory_arguments(my_func)
        assert arguments == ['a', 'b']

    def should_include_mandatory_kwargs(self):
        def my_func(a, b, *, c, d=7):
            pass
        arguments = ds._find_mandatory_arguments(my_func)
        assert arguments == ['a', 'b', 'c']


class TestValidateCategory:

    def should_return_full_category_name(self):
        assert ds._validate_category('bse') == 'base'
        assert ds._validate_category('models') == 'models'

    def should_fail_when_invalid_category(self):
        with pytest.raises(ValueError):
            ds._validate_category('invalid_category')

    def should_fail_when_none_category(self):
        with pytest.raises(ValueError):
            ds._validate_category(None)


class TestRetrieveCategory:

    def should_return_full_category_from_name(self):
        assert ds._retrieve_category('ftr_misc_dataset') == 'features'
        assert ds._retrieve_category('raw_other_dataset') == 'raw'

    def should_fail_when_invalid_name(self):
        with pytest.raises(ValueError):
            ds._retrieve_category('invalid_dataset')


class TestAbstractDataset:

    def should_register_all_subclasses(self):
        a = AbstractDataset('raw_name1')
        b = FileDataset('cln_name2', relpath='misc/path/dataset.csv')
        assert a in AbstractDataset.registry
        assert b in AbstractDataset.registry

    def should_enforce_valid_names(self):
        a = AbstractDataset('cln_name', 'some description')
        with pytest.raises(ValueError):
            b = AbstractDataset('misc_name', 'some description')

    def should_infer_category_from_name(self):
        a = AbstractDataset('cln_name', 'some description')
        assert a.category == 'clean'

    def should_not_register_classes_with_invalid_name(self):
        invalid_name = 'this_invalid_name_should_prevent_registering'
        try:
            a = AbstractDataset(invalid_name)
        except:
            pass
        registry = AbstractDataset.registry
        dataset_names = [dataset.name for dataset in registry]
        assert invalid_name not in dataset_names


class TestFileDataset:

    def should_fail_to_write(self):
        a = FileDataset('raw_dataset', relpath='raw/dataset.csv')
        df = pd.DataFrame({'a': [1, 2]})
        with pytest.raises(NotImplementedError):
            a.write(df)

    def should_fail_to_read(self):
        a = FileDataset('cln_dataset', relpath='cln/dataset.csv')
        with pytest.raises(NotImplementedError):
            df = a.read()

    def should_fail_to_tell_full_path(self):
        a = FileDataset('ftr_dataset', create=lambda x: x)
        with pytest.raises(NotImplementedError):
            a.path

    def should_fail_to_tell_last_update_time(self):
        a = FileDataset('ftr_dataset', create=lambda x: x)
        with pytest.raises(NotImplementedError):
            a.last_update_time()

    def should_fail_to_tell_if_exists(self):
        a = FileDataset('ftr_dataset', create=lambda x: x)
        with pytest.raises(NotImplementedError):
            a.exists()

    def should_infer_missing_relpath_from_name(self):
        a = FileDataset('ftr_dataset', create=lambda x: x)
        a._relpath.as_posix() == 'features/ftr_dataset.dat'

    def should_require_relpath_or_create(self):
        with pytest.raises(ValueError):
            a = FileDataset('mdl_dataset')

    def should_infer_parents_from_create(self):
        def create(a, b):
            return a
        a = FileDataset('ftr_dataset', create=create)
        assert a.parents == ['a', 'b']

    def should_let_override_parents(self):
        a = FileDataset(
            'ftr_dataset', create=lambda x, y: x, parents=['a', 'b'])
        assert a.parents == ['a', 'b']

    def should_have_no_parents_if_no_create(self):
        a = FileDataset('ftr_dataset', relpath='features/dataset.csv')
        assert a.parents is None

    def should_fail_if_create_incompatible_with_parents(self):
        with pytest.raises(ValueError):
            a = FileDataset(
                'ftr_dataset', create=lambda x, y: x, parents=['a'])

    def should_be_instanciable_with_decorator(self):

        @FileDataset.from_parents(description='mydesc')
        def cln_dataset_from_decorator(raw_dataset_a, raw_dataset_b):
            return raw_dataset_a

        # Find the dataset created in the registry
        registry = AbstractDataset.registry
        print(registry[-1], dir(registry[-1]))
        dataset_names = [dataset.name for dataset in registry]
        assert 'cln_dataset_from_decorator' in dataset_names


class TestCsvDataset:

    def should_write(self, tmpdir):
        FileDataset.file_system = LocalFileSystem(tmpdir)
        a = CsvDataset('raw_dataset', relpath='raw/dataset.csv')
        df = pd.DataFrame({'a': [1, 2]})
        a.write(df)
        # Check what has been written
        imported_df = pd.read_csv(a.path, index_col=0)
        assert df.equals(imported_df)

    def should_write_with_kwargs(self, tmpdir):
        FileDataset.file_system = LocalFileSystem(tmpdir)
        a = CsvDataset(
            'raw_dataset', relpath='raw/dataset.csv',
            write_kwargs={'index': False})
        df = pd.DataFrame({'a': [1, 2]})
        a.write(df)
        # Check what has been written
        imported_df = pd.read_csv(a.path)
        assert df.equals(imported_df)

    def should_read(self):
        fs_root = Path(__file__).parent/'examples/datasets'
        FileDataset.file_system = LocalFileSystem(fs_root)
        a = CsvDataset('raw_dataset', relpath='raw_dataset.csv')
        df = a.read()
        assert 'a' in df.columns
        assert df.shape == (2, 3)

    def should_read_with_kwargs(self):
        fs_root = Path(__file__).parent/'examples/datasets'
        FileDataset.file_system = LocalFileSystem(fs_root)
        a = CsvDataset(
            'raw_dataset', relpath='raw_dataset.csv',
            read_kwargs={'index_col': 'a'})
        df = a.read()
        assert 'a' not in df.columns
        assert df.index.name == 'a'
        assert df.shape == (2, 2)

    def should_tell_full_path(self, tmpdir):
        FileDataset.file_system = LocalFileSystem(tmpdir)
        a = CsvDataset('ftr_dataset', create=lambda x: x)
        assert a.path == (Path(tmpdir)/'features'/'ftr_dataset.csv')

    def should_tell_last_update_time(self, tmpdir):
        FileDataset.file_system = LocalFileSystem(tmpdir)
        a = CsvDataset('ftr_dataset', create=lambda x: x)
        assert a.last_update_time() == 0
        df = pd.DataFrame({'a': [1, 2]})
        a.write(df)
        last_update_time = a.last_update_time()
        assert (last_update_time != 0) and isinstance(last_update_time, float)

    def should_tell_if_exists(self):
        fs_root = Path(__file__).parent/'examples/datasets'
        FileDataset.file_system = LocalFileSystem(fs_root)
        a = CsvDataset('raw_dataset', relpath='raw_dataset.csv')
        assert a.exists()
        a = CsvDataset('raw_dataset', relpath='non_existant_dataset.csv')
        assert not a.exists()

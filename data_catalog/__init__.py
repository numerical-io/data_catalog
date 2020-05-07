# from .core.catalog import DataCatalog
# from .core.datasets import FileDataset, CsvDataset, \
#     ParquetDataset, PickleDataset
# from .core.file_systems import LocalFileSystem, S3FileSystem, \
#     create_filesystem_from_path
# from .core.pipeline import Pipeline
# 
#
# def get_catalog(fs_root, **fs_kwargs):
#     """Return a catalog containing all datasets defined in the current session.
#
#     Args:
#         fs_root (str): root of the file system. This can be a local path, or a
#             path on S3. Paths on S3 must start with `s3://`.
#         fs_kwargs (dict): dictionary of parameters necessary for the file
#             system. For S3, this can contain access keys, for instance (as
#             defined in s3fs).
#     Returns:
#         DataCatalog: a catalog containing all datasets defined in the current
#             session. All datasets objects are automatically registered at
#             creation ; the function uses this registry to create the catalog.
#     """
#     # Set the file system from the root path
#     FileDataset.file_system = create_filesystem_from_path(fs_root, **fs_kwargs)
#     # Create a catalog with all registered datasets
#     catalog = DataCatalog.from_list(FileDataset.registry)
#     return catalog

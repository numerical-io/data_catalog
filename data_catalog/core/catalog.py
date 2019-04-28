"""Data catalog structure, to hold datasets.

"""

class DataCatalog(dict):

    def describe(self):
        desc = {k: v.description for k, v in self.items()}
        return desc

    def list(self):
        """List the datasets in the catalog.
        """
        return list(self.keys())

    def add(self, dataset):
        self[dataset.name] = dataset

    @property
    def ls(self):
        """Shortcut to .list()
        """
        return self.list()

    @classmethod
    def from_list(cls, list_of_datasets):
        data_catalog = cls()
        for dataset in list_of_datasets:
            data_catalog.add(dataset)
        return data_catalog

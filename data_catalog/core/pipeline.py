"""Data pipeline to compute datasets based on a data catalog.

"""
import logging

from dask.multiprocessing import get
import dask


logger = logging.getLogger('data_catalog')


def _create_task(catalog, target):
    """
    """
    parents = catalog[target].parents

    def task(*args):
        """

        Arguments of this function are set by Dask but not used, because data
        is passed on disk rather than in memory.
        """
        # Do nothing if the dataset has no parent : the dataset cannot change
        if not parents:
            return

        # Check whether the dataset needs updating
        target_date = catalog[target].last_update_time()
        max_parent_date = max([
            catalog[parent].last_update_time()
            for parent in parents])
        needs_update = target_date < max_parent_date

        if needs_update:
            logger.info('Create dataset {}'.format(target))

            inputs = [catalog[parent].read() for parent in parents]
            df = catalog[target].create(*inputs)
            catalog[target].write(df)

            logger.info('Done dataset {}'.format(target))

    return (task, parents)


class Pipeline:

    def __init__(self, data_catalog):
        self.tasks_graph = self._create_task_graph(data_catalog)

    def run(self, targets=None):
        # Run pipeline
        if not targets:
            targets = list(self.tasks_graph.keys())
        return get(self.tasks_graph, targets)

    def visualize(self, filename):
        return dask.visualize(self.tasks_graph, filename=filename)

    def _create_task_graph(self, data_catalog):
        tasks_graph = {}
        for key, dataset in data_catalog.items():
            tasks_graph[key] = _create_task(data_catalog, key)
        return tasks_graph

import logging
from datetime import datetime

import dask
import dask.optimization
from dask.core import toposort

from .abc import is_dataset, is_collection, is_collection_filter


logger = logging.getLogger(__name__)


def __create_task(dataset, resolved_parents, in_memory_data_transfer=False):
    """Create a dataset from its parents, and write it.
    """

    def in_memory_task(args):
        logger.info("CREATE {}".format(dataset.catalog_path()))
        df = dataset.create(*args)
        dataset.write(df)
        return df

    def from_storage_task(args):
        # Load inputs from storage
        inputs = []
        for parent in resolved_parents:
            logger.info(
                "CREATE {} <- READ {}".format(
                    dataset.catalog_path(), parent.catalog_path()
                )
            )
            inputs.append(parent.read())

        # Plug the inputs into the in-memory task
        df = in_memory_task(inputs)
        return None

    if in_memory_data_transfer:
        return (in_memory_task, resolved_parents)
    else:
        return (from_storage_task, resolved_parents)


def __collect_task(collection_instance, in_memory_data_transfer=False):
    """Link a collection to its items, in the task graph.

    When data transfers do not happen in memory, the task does nothing
    because its results will not be transfered anyway. It is necessary
    nonetheless, to register the parent-child dependency in the task graph.
    """
    context = collection_instance.context
    ordered_keys = list(collection_instance.keys())
    parents = [collection_instance.get(key)(context) for key in ordered_keys]

    def task(args):
        if in_memory_data_transfer:
            logger.info("COLLECT {}".format(collection_instance.catalog_path()))
            return dict(zip(ordered_keys, args))
        else:
            return None

    return (task, parents)


def __read_task(dataset, in_memory_data_transfer=False):
    """Read a dataset.

    When data transfers do not happen in memory, the task does nothing
    because its results will not be transfered anyway. It is necessary
    nonetheless, to register the parent-child dependency in the task graph.
    """

    def task():
        logger.info("READ {}".format(dataset.catalog_path()))
        return dataset.read()

    if in_memory_data_transfer:
        return (task,)
    else:
        return None


def _get_dataset_instances(datasets_and_collections, context):
    """Create instances of all datasets, including the ones in collections.
    """
    all_datasets = set()
    for d in datasets_and_collections:
        if is_dataset(d):
            all_datasets.add(d(context))
        if is_collection(d):
            for key in d(context).keys():
                all_datasets.add(d.get(key)(context))

    return all_datasets


def _create_task_graph(datasets, context, in_memory_data_transfer=False):
    """Create the task graph spanning all datasets.

    In the task graph, all datasets are instances (not classes), whether the
    are child or parent.
    """
    task_graph = {}
    for dataset in datasets:
        # When a parent is a filter, resolve the filter (it resolves into a
        # dataset or collection)
        resolved_parents = []
        for parent in dataset.parents:
            if is_collection_filter(parent):
                resolved_parents.append(parent.filter_by(dataset.key)(context))
            else:
                resolved_parents.append(parent(context))

        # Add the dataset creation to the graph
        task_graph[dataset] = __create_task(
            dataset,
            resolved_parents,
            in_memory_data_transfer=in_memory_data_transfer,
        )

        # When a parent is a collection, add a task to build the collection
        # from its datasets. Do it in this case, because the task graph
        # needs it ; but don't do it for collections that do not need to be
        # collected at once since they may not hold in memory.
        for parent in resolved_parents:
            if is_collection(parent):
                task_graph[parent] = __collect_task(
                    parent, in_memory_data_transfer=in_memory_data_transfer,
                )

    return task_graph


def _prune_task_graph(task_graph, target_datasets):
    """Prune tasks that are not necessary to compute targets.

    Args:
        task_graph (dict): dask-style task graph.
        target_datasets (list of dataset instances): datasets that must be
            computed.
    """
    new_task_graph, _ = dask.optimization.cull(task_graph, target_datasets)
    return new_task_graph


def _prevent_update_of_unchanging_datasets(
    task_graph, in_memory_data_transfer=False
):
    """Modify the task graph to prevent computing datasets that will not change.

    The resulting task graph can be optimized by pruning parts that have become
    disconnected from the computation of targets.
    """
    sorted_data_objects = toposort(task_graph)

    # A data object will be added to data_objects_to_update if it needs
    # updating. Otherwise, its last update time will be recorded in
    # last_update_times. Because we address data objects in topological sort
    # order, the parents of an object will always be available in one of
    # data_objects_to_update or last_update_times (never in both though).
    data_objects_to_update = set()
    last_update_times = {}
    for data_object in sorted_data_objects:
        _, parents = task_graph[data_object]
        parents_to_update = data_objects_to_update.intersection(parents)
        has_parents_to_update = bool(parents_to_update)

        # The order of tests below tries to minimize access to storage,
        # which can be time-consuming (depending on storage type and graph size)

        # If it's a collection: update if any member item must be updated
        if is_collection(data_object):
            requires_update = has_parents_to_update
            if not requires_update:
                # Calculate last update times for the collection
                update_time_parents = {last_update_times[p] for p in parents}
                last_update_times[data_object] = max(
                    update_time_parents,
                    default=datetime.fromtimestamp(0).astimezone(),
                )

        # If it's not a collection, then it's a dataset
        elif has_parents_to_update or (not data_object.exists()):
            requires_update = True

        else:
            # The dataset exists and none of its parents must be updated.
            # Yet it will need an update if one of its parents has a later
            # update time.
            update_time_parents = {last_update_times[p] for p in parents}
            t = data_object.last_update_time()
            requires_update = any({tp > t for tp in update_time_parents})
            if not requires_update:
                # Save the last update time
                last_update_times[data_object] = t

        if requires_update:
            data_objects_to_update.add(data_object)

    # For datasets that will not change, the "create dataset" task is replaced
    # by a "read from storage" task without parents. For collections, the
    # "collect" task remains the same, whether the collection needs updating
    # or not.
    unchanging_objects = set(task_graph.keys()).difference(
        data_objects_to_update
    )
    unchanging_datasets = {d for d in unchanging_objects if is_dataset(d)}
    for dataset in unchanging_datasets:
        task_graph[dataset] = __read_task(
            dataset, in_memory_data_transfer=in_memory_data_transfer
        )

    return task_graph


def create_task_graph(
    data_classes, context, targets=None, in_memory_data_transfer=False
):
    """Create a task graph, optimized to compute targets.

    Args:
        data_classes (list of datasets or collections): All catalog classes
            involved in the computation of targets (directly or through
            dependencies).
        context (dict): Catalog context.
        targets (list of datasets or collections): Catalog classes that must be
            computed. If None, all items in data_classes are computed.
        in_memory_data_transfer (bool): If True, let Dask transfer outputs of a
            task into inputs of the next, in memory. If False, each task reads
            its inputs from storage, and values transferred by Dask are set to
            None. This reduces the memory footprint of the application, at the
            expense of more storage accesses.
    """
    # Create dataset instances from dataset/collection classes in inputs
    logger.info("Create task graph")
    all_datasets = _get_dataset_instances(data_classes, context)
    if targets:
        target_datasets = list(_get_dataset_instances(targets, context))
    else:
        target_datasets = list(all_datasets)

    # Create the task graph
    task_graph = _create_task_graph(
        all_datasets, context, in_memory_data_transfer=in_memory_data_transfer
    )

    # Optimize the task graph, first by restricting to the subgraph useful to
    # compute targets, then by removing datasets that will not change. The first
    # pruning reduces storage access when detecting unchanging datasets.
    logger.info("Optimize task graph")
    task_graph = _prune_task_graph(task_graph, target_datasets)
    task_graph = _prevent_update_of_unchanging_datasets(
        task_graph, in_memory_data_transfer=in_memory_data_transfer
    )
    task_graph = _prune_task_graph(task_graph, target_datasets)

    return task_graph, target_datasets

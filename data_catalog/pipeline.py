from .abc import is_collection
import pandas as pd

# Example of simple recursive update of items (not optimized)
def update_and_get(target, context):

    print(pd.Timestamp.now(), target.catalog_path())

    # If target is a collection, call this function on each collection item
    if is_collection(target):
        keys = target(context).keys()
        collection_last_updates = []
        collection_dfs = {}
        for key in keys:
            df, last_update = update_and_get(target.get(key), context)
            collection_dfs[key] = df
            collection_last_updates.append(last_update)
        return collection_dfs, max(collection_last_updates)

    # At this point, target is always a dataset (not a collection)
    target_ds = target(context)
    target_date = target_ds.last_update_time()
    parent_dfs = []

    parents = target_ds.parents
    if not parents:
        needs_update = not target_ds.exists()

    else:
        # Ask to update parents (if necessary)
        max_parent_date = None
        for parent in parents:
            parent_df, parent_last_update = update_and_get(parent, context)
            parent_dfs.append(parent_df)
            if max_parent_date is None:
                max_parent_date = parent_last_update
            else:
                max_parent_date = max(max_parent_date, parent_last_update)

        needs_update = target_date < max_parent_date

    if needs_update:
        df = target_ds.create(*parent_dfs)
        target_ds.write(df)
    else:
        df = target_ds.read()
    return df, target_ds.last_update_time()

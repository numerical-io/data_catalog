# Data Catalog

## Usage

```python
from data_catalog import get_catalog, CsvDataset, \
    ParquetDataset, PickleDataset, Pipeline

# Define a raw CSV dataset
a = CsvDataset('raw_dataset_a', relpath='raw/dataset_a.csv')

# Define a transformed Parquet dataset
@ParquetDataset.from_parents('Cleaned dataset A')
def cln_dataset_a(raw_dataset_a):
    return raw_dataset_a.dropna()

# Load a catalog containing all datasets defined so far
data_catalog = get_catalog('/path/to/datasets')
data_catalog.describe()

# Generate all datasets using the pipeline
# The pipeline will not regenerate an existing file, unless
# its parents have a more recent modification time.
Pipeline(data_catalog).run()

# Read a dataset
df = data_catalog['cln_dataset_a'].read()
```

## Data layers

This package enforces the attribution of datasets to layers, from raw datasets to features, models, and results.

The layers (and their abbreviation) are:
<dl>
<dt>raw (raw)</dt>
<dd>Original, untransformed datasets.</dd>
<dt>clean (cln)</dt>
<dd>Datasets in the same format as raw originals, only cleaned (data types set, variable names adjusted to conventions, NaN values identified as such, etc.)</dd>
<dt>base (bse)</dt>
<dd>Datasets, still in their initial data domain, transformed in a convenient base for the creation of features.</dd>
<dt>features (ftr)</dt>
<dd>Features for the models, where lines correspond to model samples.</dd>
<dt>inputs (inp)</dt>
<dd>Datasets formed from the concatenation of features, specific to a given model.</dd>
<dt>models (mdl)</dt>
<dd>Calibrated models.</dd>
<dt>results (res)</dt>
<dd>Results from models or data analyses.</dd>
</dl>

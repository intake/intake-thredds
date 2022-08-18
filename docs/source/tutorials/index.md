---
jupytext:
  text_representation:
    format_name: myst
kernelspec:
  display_name: Python 3
  name: python3
---

# Tutorial

Intake-thredds provides an interface that combines functionality from [`siphon`](https://github.com/Unidata/siphon) and `intake` to retrieve data from THREDDS data servers. This tutorial provides an introduction to the API and features of intake-thredds. Let's begin by importing `intake`.

```{code-cell} ipython3
import intake
```

## Loading a catalog

You can load data from a THREDDS catalog by providing the URL to a valid THREDDS catalog:

```{code-cell} ipython3
cat_url = 'https://psl.noaa.gov/thredds/catalog/Datasets/noaa.ersst/catalog.xml'
catalog = intake.open_thredds_cat(cat_url, name='noaa-ersst-catalog')
catalog
```

## Using the catalog

Once you've loaded a catalog, you can display its contents by iterating over its entries:

```{code-cell} ipython3
list(catalog)
```

Once you've identified a dataset of interest, you can access it as follows:

```{code-cell} ipython3
source = catalog['err.mnmean.v3.nc']
print(source)
```

## Loading a dataset

To load a dataset of interest, you can use the `to_dask()` method which is available on a **source** object:

```{code-cell} ipython3
%%time
ds = source().to_dask()
ds
```

The `to_dask()` reads only metadata needed to construct an `xarray.Dataset`. The actual data are streamed over the network when computation routines are invoked on the dataset.
By default, `intake-thredds` uses `chunks={}` to load the dataset with dask using a single chunk for all arrays. You can use a different chunking scheme by prividing a custom value of chunks before calling `.to_dask()`:

```{code-cell} ipython3
%%time
# Use a custom chunking scheme
ds = source(chunks={'time': 100, 'lon': 90}).to_dask()
ds
```

## Working with nested catalogs

In some scenarious, a THREDDS catalog can reference another THREDDS catalog. This results into a nested structure consisting of a parent catalog and children catalogs:

```{code-cell} ipython3
cat_url = 'https://psl.noaa.gov/thredds/catalog.xml'
catalog = intake.open_thredds_cat(cat_url)
list(catalog)
```

```{code-cell} ipython3
print(list(catalog['Datasets']))
```

```{code-cell} ipython3
print(list(catalog['Datasets']['ncep.reanalysis.dailyavgs']))
```

To load data from such a nested catalog, `intake-thredds` provides a special source object {py:class}`~intake_thredds.source.THREDDSMergedSource` accessible via the `.open_thredds_merged()` function. The inputs for this function consists of:

- `url`: top level URL of the THREDDS catalog
- `path`: a list of paths for child catalogs to descend down. The paths can include glob characters (\*, ?). These glob characters are used for matching.

```{code-cell} ipython3
source = intake.open_thredds_merged(
    cat_url, path=['Datasets', 'ncep.reanalysis.dailyavgs', 'surface', 'air*sig995*194*.nc']
)
print(source)
```

To load the data into an xarray {py:class}`~xarray.Dataset`, you can invoke the `.to_dask()` method.
Internally, {py:class}`~intake_thredds.source.THREDDSMergedSource` does the following:

- descend down the given paths and collect all available datasets.
- load each dataset in a dataset.
- combine all loaded datasets into a single dataset.

```{code-cell} ipython3
%%time
ds = source.to_dask()
ds
```

## Caching

Under the hood `intake-thredds` uses the `driver='opendap'` from `intake-xarray` by default. You can also choose
`driver='netcdf'`, which in combination with `fsspec` caches files by appending `simplecache::` to the url,
see [fsspec docs](https://filesystem-spec.readthedocs.io/en/latest/features.html#remote-write-caching).

```{code-cell} ipython3
import os

import fsspec

# specify caching location, where to store files to with their original names
fsspec.config.conf['simplecache'] = {'cache_storage': 'my_caching_folder', 'same_names': True}

cat_url = 'https://psl.noaa.gov/thredds/catalog.xml'
source = intake.open_thredds_merged(
    f'simplecache::{cat_url}',
    path=['Datasets', 'ncep.reanalysis.dailyavgs', 'surface', 'air.sig995.194*.nc'],
    driver='netcdf',  # specify netcdf driver to open HTTPServer
)
print(source)
```

```{code-cell} ipython3
%time ds = source.to_dask()
```

```{code-cell} ipython3
assert os.path.exists('my_caching_folder/air.sig995.1949.nc')
```

```{code-cell} ipython3
# after caching very fast
%time ds = source.to_dask()
```

## Multi-file `concat_kwargs` with `cfgrib` engine

Another example demonstrating how to use caching consists of reading in ensemble members in parallel for GEFS. The example below reads in 21 ensemble members for a single timestep. It also demonstrates usage of `xarray_kwargs` which are passed on to `xarray` for opening the files, which in this cases uses the `cfgrib` engine:

```{code-cell} ipython3
cat_url = 'https://www.ncei.noaa.gov/thredds/catalog/model-gefs-003/202008/20200831/catalog.xml'
source = intake.open_thredds_merged(
    f'simplecache::{cat_url}',
    path=["NCEP gens-a Grid 3 Member-Forecast *-372 for 2020-08-31 00:00*"],
    driver="netcdf",
    concat_kwargs={"dim": "number"},
    xarray_kwargs=dict(
        engine="cfgrib",
        backend_kwargs=dict(
            filter_by_keys={"typeOfLevel": "heightAboveGround", "cfVarName": "t2m"}
        ),
    ),
)
source.to_dask()
```

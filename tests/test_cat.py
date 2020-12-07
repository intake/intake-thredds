import intake
import pytest
import xarray as xr


@pytest.fixture(scope='module')
def thredds_cat_url():
    return 'https://psl.noaa.gov/thredds/catalog/Datasets/noaa.ersst/catalog.xml'


def test_init_catalog(thredds_cat_url):
    cat = intake.open_thredds_cat(thredds_cat_url)
    assert isinstance(cat, intake.catalog.Catalog)
    assert cat.metadata == cat.cat.metadata
    assert cat.discover()['container'] == 'catalog'

    assert 'err.mnmean.v3.nc' in cat

    cat = intake.open_thredds_cat(thredds_cat_url, metadata={'random_attribute': 'thredds'})
    assert 'random_attribute' in cat.metadata


def test_entry(thredds_cat_url):
    cat = intake.open_thredds_cat(thredds_cat_url)
    entry = cat['err.mnmean.v3.nc']
    assert isinstance(entry, intake.source.base.DataSource)
    d = entry.describe()
    assert d['name'] == 'err.mnmean.v3.nc'
    assert d['container'] == 'xarray'
    assert d['plugin'] == ['netcdf']
    assert (
        d['args']['urlpath']
        == 'https://psl.noaa.gov/thredds/dodsC/Datasets/noaa.ersst/err.mnmean.v3.nc'
    )

    ds = entry(chunks={}).to_dask()
    assert isinstance(ds, xr.Dataset)

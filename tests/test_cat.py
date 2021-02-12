import intake
import intake_xarray
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


@pytest.mark.parametrize('driver', ['netcdf', 'opendap'])
def test_entry(thredds_cat_url, driver):
    """Test entry.to_dask() is xr.Dataset and allows opendap and netcdf as source."""
    cat = intake.open_thredds_cat(thredds_cat_url, driver=driver)
    entry = cat['err.mnmean.v3.nc']
    if driver == 'opendap':
        assert isinstance(entry, intake_xarray.opendap.OpenDapSource)
    elif driver == 'netcdf':
        assert isinstance(entry, intake_xarray.netcdf.NetCDFSource)
    d = entry.describe()
    assert d['name'] == 'err.mnmean.v3.nc'
    assert d['container'] == 'xarray'
    assert d['plugin'] == [driver]
    if driver == 'opendap':
        loc = 'dodsC'
    elif driver == 'netcdf':
        loc = 'fileServer'
    assert (
        d['args']['urlpath']
        == f'https://psl.noaa.gov/thredds/{loc}/Datasets/noaa.ersst/err.mnmean.v3.nc'
    )
    ds = entry(chunks={}).to_dask()
    assert isinstance(ds, xr.Dataset)


def test_entry_simplecache_netcdf(thredds_cat_url):
    """Test allow simplecache:: in url if netcdf as source."""
    import os

    import fsspec

    fsspec.config.conf['simplecache'] = {'cache_storage': 'my_caching_folder', 'same_names': True}
    cat = intake.open_thredds_cat(f'simplecache::{thredds_cat_url}', driver='netcdf')
    entry = cat['err.mnmean.v3.nc']
    ds = entry(chunks={}).to_dask()
    assert isinstance(ds, xr.Dataset)
    # test files present
    os.path.exists('my_caching_folder/err.mnmean.v3.nc')


def test_entry_simplecache_fails_opendap(thredds_cat_url):
    """Test no simplecache:: in url with opendap."""
    with pytest.raises(ValueError) as e:
        intake.open_thredds_cat(f'simplecache::{thredds_cat_url}', driver='opendap')
    assert 'simplecache requires driver="netcdf"' in str(e.value)

import intake
import intake_xarray
import pytest
import xarray as xr


@pytest.fixture(scope='module')
def thredds_cat_url():
    """Single file thredds catalog."""
    return 'https://psl.noaa.gov/thredds/catalog/Datasets/noaa.ersst/catalog.xml'


def test_ThreddsCatalog_init_catalog(thredds_cat_url):
    """Test initialization of ThreddsCatalog."""
    cat = intake.open_thredds_cat(thredds_cat_url)
    assert isinstance(cat, intake.catalog.Catalog)
    assert cat.metadata == cat.cat.metadata
    assert cat.discover()['container'] == 'catalog'

    assert 'err.mnmean.v3.nc' in cat

    cat = intake.open_thredds_cat(thredds_cat_url, metadata={'random_attribute': 'thredds'})
    assert 'random_attribute' in cat.metadata


@pytest.mark.parametrize('driver', ['netcdf', 'opendap'])
def test_ThreddsCatalog(thredds_cat_url, driver):
    """Test entry.to_dask() is xr.Dataset and allows opendap and netcdf as source."""
    cat = intake.open_thredds_cat(thredds_cat_url, driver=driver)
    entry = cat['sst.mon.19712000.ltm.v3.nc']
    if driver == 'opendap':
        assert isinstance(entry, intake_xarray.opendap.OpenDapSource)
    elif driver == 'netcdf':
        assert isinstance(entry, intake_xarray.netcdf.NetCDFSource)
    d = entry.describe()
    assert d['name'] == 'sst.mon.19712000.ltm.v3.nc'
    assert d['container'] == 'xarray'
    assert d['plugin'] == [driver]
    if driver == 'opendap':
        loc = 'dodsC'
    elif driver == 'netcdf':
        loc = 'fileServer'
    assert (
        d['args']['urlpath']
        == f'https://psl.noaa.gov/thredds/{loc}/Datasets/noaa.ersst/sst.mon.19712000.ltm.v3.nc'
    )
    ds = entry(chunks={}).to_dask()
    assert isinstance(ds, xr.Dataset)


def test_ThreddsCatalog_simplecache_netcdf(thredds_cat_url):
    """Test that ThreddsCatalog allows simplecache:: in url if netcdf as source."""
    import os

    import fsspec

    fsspec.config.conf['simplecache'] = {'cache_storage': 'my_caching_folder', 'same_names': True}
    cat = intake.open_thredds_cat(f'simplecache::{thredds_cat_url}', driver='netcdf')
    entry = cat['sst.mon.19712000.ltm.v3.nc']
    ds = entry(chunks={}).to_dask()
    assert isinstance(ds, xr.Dataset)
    # test files present
    cached_file = 'my_caching_folder/sst.mon.19712000.ltm.v3.nc'
    assert os.path.exists(cached_file)
    os.remove(cached_file)
    assert not os.path.exists(cached_file)


def test_ThreddsCatalog_simplecache_fails_opendap(thredds_cat_url):
    """Test that ThreddsCatalog simplecache:: in url with opendap."""
    with pytest.raises(ValueError, match=r'simplecache requires driver="netcdf"'):
        intake.open_thredds_cat(f'simplecache::{thredds_cat_url}', driver='opendap')


def test_ThreddsCatalog_intake_xarray_kwargs():
    """Test that ThreddsCatalog allows intake_xarray kwargs."""
    cat = intake.open_thredds_cat(
        'https://psl.noaa.gov/thredds/catalog/Datasets/noaa.ersst/catalog.xml',
        driver='netcdf',
        intake_xarray_kwargs={
            'xarray_kwargs': {'decode_times': False, 'engine': 'h5netcdf'},
            'chunks': {},
        },
    )
    entry = cat['sst.mon.19712000.ltm.v3.nc']
    ds = entry(chunks={}).to_dask()
    assert isinstance(ds, xr.Dataset)

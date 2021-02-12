import intake
import pytest
import xarray as xr


@pytest.fixture(scope='module')
def THREDDSMergedSource_cat():
    """THREDDSMergedSource looping through folders."""
    thredds_cat_url = 'https://psl.noaa.gov/thredds/catalog.xml'
    paths = [
        'Datasets',
        'ncep.reanalysis.dailyavgs',
        'surface',
        'air*sig995*194*.nc',
    ]
    cat = intake.open_thredds_merged(thredds_cat_url, paths)
    assert cat.urlpath == thredds_cat_url
    assert cat.path == paths
    return cat

@pytest.fixture(scope='module')
def THREDDSMergedSource_cat_short_url():
    return 'https://psl.noaa.gov/thredds/catalog/Datasets/ncep.reanalysis.dailyavgs/surface/catalog.xml'

@pytest.fixture(scope='module')
def THREDDSMergedSource_cat_short_path():
    return ['air.sig995*194*.nc']  # todo: convert . to * ?


@pytest.fixture(scope='module')
def THREDDSMergedSource_cat_short(THREDDSMergedSource_cat_short_url, THREDDSMergedSource_cat_short_path):
    """THREDDSMergedSource without the looping faster."""
    cat = intake.open_thredds_merged(THREDDSMergedSource_cat_short_url, THREDDSMergedSource_cat_short_path)
    assert cat.urlpath == THREDDSMergedSource_cat_short_url
    assert cat.path == THREDDSMergedSource_cat_short_path
    return cat


@pytest.fixture(scope='module')
def THREDDSMergedSource_cat_short_simplecache(THREDDSMergedSource_cat_short_url, THREDDSMergedSource_cat_short_path):
    """Without the looping faster."""
    return intake.open_thredds_merged(f'simplecache::{THREDDSMergedSource_cat_short_url}', THREDDSMergedSource_cat_short_path, driver='netcdf')


def test_THREDDSMergedSource(THREDDSMergedSource_cat):
    cat = THREDDSMergedSource_cat
    ds = cat.to_dask()
    assert dict(ds.dims) == {'lat': 73, 'lon': 144, 'time': 731}
    d = cat.discover()
    assert set(d['metadata']['coords']) == set(('lat', 'lon', 'time'))
    assert set(d['metadata']['data_vars'].keys()) == set(['air'])


def test_THREDDSMergedSource_long_short(THREDDSMergedSource_cat, THREDDSMergedSource_cat_short):
    ds = THREDDSMergedSource_cat.to_dask()
    ds_short = THREDDSMergedSource_cat_short.to_dask()
    xr.testing.assert_equal(ds, ds_short)


def test_THREDDSMergedSource_simplecache_netcdf(THREDDSMergedSource_cat_short_simplecache):
    """Test that THREDDSMergedSource allows simplecache:: in url if netcdf as source."""
    import os

    import fsspec
    cache_storage = 'my_caching_folder'
    fsspec.config.conf['simplecache'] = {'cache_storage': cache_storage, 'same_names': True}
    cat = THREDDSMergedSource_cat_short_simplecache
    ds = cat.to_dask()
    assert isinstance(ds, xr.Dataset)
    # test files present
    cached_files = ['air.sig995.1948.nc', 'air.sig995.1949.nc']
    for f in cached_files:
        cached_file = os.path.join(cache_storage, f)
        assert os.path.exists(cached_file)
        os.remove(cached_file)
        assert not os.path.exists(cached_file)


def test_THREDDSMergedSource_simplecache_fails_opendap(THREDDSMergedSource_cat_short_url):
    """Test that THREDDSMergedSource simplecache:: in url with opendap."""
    with pytest.raises(ValueError) as e:
        intake.open_thredds_cat(f'simplecache::{THREDDSMergedSource_cat_short_url}', driver='opendap')
    assert 'simplecache requires driver="netcdf"' in str(e.value)

import intake
import pytest


@pytest.fixture(scope='module')
def thredds_cat_url():
    return 'http://dap.nci.org.au/thredds/catalog.xml'


def test_thredds_merge(thredds_cat_url):
    paths = [
        'eMAST TERN',
        'eMAST TERN - files',
        'ASCAT',
        'ASCAT_v1-0_soil-moisture_daily_0-05deg_2007-2011',
        '00000000',
        '*12.nc',  # to speed up only takes all december files
    ]
    cat = intake.open_thredds_merged(thredds_cat_url, paths)
    assert cat.urlpath == thredds_cat_url
    assert cat.path == paths

    ds = cat.to_dask()
    assert dict(ds.dims) == {'lat': 681, 'lon': 841, 'time': 155}
    d = cat.discover()
    assert set(d['metadata']['coords']) == set(('lat', 'lon', 'time'))
    assert set(d['metadata']['data_vars'].keys()) == set(
        ['crs', 'lwe_thickness_of_soil_moisture_content']
    )

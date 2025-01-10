from intake.readers import Service
from intake.readers.catalogs import THREDDSCatalogReader


class ThreddsCatalog:
    """Intake catalog interface to a thredds catalog."""

    def __new__(cls, url: str, driver: str = 'opendap', intake_xarray_kwargs=None, metadata=None):
        """
        Parameters
        ----------
        url : str
            Location of thredds catalog.
        driver : str
            Select driver to access data. Choose from 'netcdf' and 'opendap'.
        intake_xarray_kwargs : dict
            Keyword arguments to pass to intake_xarray DataSource.
        **kwargs :
            Additional keyword arguments are passed through to the
            :py:class:`~intake.catalog.Catalog` base class.

        Examples
        --------
        >>> import intake
        >>> cat_url = 'https://psl.noaa.gov/thredds/catalog/Datasets/noaa.ersst/catalog.xml'
        >>> cat = intake.open_thredds_cat(cat_url)
        """

        simplecache = url.startswith('simplecache:')
        if simplecache and driver == 'opendap':
            raise ValueError('simplecache requires driver="netcdf"')
        url = url.removeprefix('simplecache::')
        data = Service(url)
        reader = THREDDSCatalogReader(data, make=driver[-3:])
        cat = reader.read()
        if metadata:
            cat.metadata.update(metadata)
        if simplecache:
            for d in cat.data.values():
                d.kwargs['url'] = 'simplecache::' + d.kwargs['url']
        if intake_xarray_kwargs:
            intake_xarray_kwargs.update(intake_xarray_kwargs.pop('xarray_kwargs', {}))
            for d in cat.entries.values():
                d.kwargs.update(intake_xarray_kwargs)
        return cat

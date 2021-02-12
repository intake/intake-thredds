from intake.catalog import Catalog
from intake.catalog.local import LocalCatalogEntry


class ThreddsCatalog(Catalog):
    name = 'thredds_cat'

    def __init__(self, url, driver='opendap', **kwargs):
        self.url = url
        self.driver = driver
        super().__init__(**kwargs)

    def _load(self):
        from siphon.catalog import TDSCatalog

        if 'simplecache::' in self.url:
            if self.driver == 'netcdf':
                use_simplecache = True
                self.url = self.url.replace('simplecache::', '')
            else:
                raise ValueError(
                    'simplecache requires driver="netcdf", found driver="{self.driver}".')
        else:
            use_simplecache = False

        self.cat = TDSCatalog(self.url)
        self.name = self.cat.catalog_name
        self.metadata.update(self.cat.metadata)

        # sub-cats
        self._entries = {
            r.title: LocalCatalogEntry(
                r.title,
                'THREDDS cat',
                'thredds_cat',
                True,
                {'url': r.href},
                [],
                [],
                {},
                None,
                catalog=self,
            )
            for r in self.cat.catalog_refs.values()
        }

        def access_urls(ds, use_simplecache, self):
            # data entries (only those with opendap links)
            if self.driver == 'opendap':
                driver_for_access_urls = 'OPENDAP'
            elif self.driver == 'netcdf':
                driver_for_access_urls = 'HTTPServer'
            url = ds.access_urls[driver_for_access_urls]
            if use_simplecache:
                url = f'simplecache::{url}'
            return url

        self._entries.update(
            {
                ds.name: LocalCatalogEntry(
                    ds.name,
                    'THREDDS data',
                    self.driver,
                    True,
                    {'urlpath': access_urls(ds, use_simplecache, self), 'chunks': None},
                    [],
                    [],
                    {},
                    None,
                    catalog=self,
                )
                for ds in self.cat.datasets.values()
            }
        )

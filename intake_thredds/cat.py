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
        #print(self.cache, self.url)

        if 'simplecache::' in self.url:
            if self.driver == 'netcdf':
                self.cache = True
                self.url_no_simplecache = self.url.replace('simplecache::', '')
                self.metadata.update({'cache':'simplecache::'})
            else:
                raise ValueError(
                    f'simplecache requires driver="netcdf", found driver="{self.driver}".'
                )
        else:
            self.cache = False
            self.url_no_simplecache = self.url

        self.cat = TDSCatalog(self.url_no_simplecache)
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
                self.metadata,
                None,
                catalog=self,
            )
            for r in self.cat.catalog_refs.values()
        }

        def access_urls(ds, self):
            # data entries (only those with opendap links)
            if self.driver == 'opendap':
                driver_for_access_urls = 'OPENDAP'
            elif self.driver == 'netcdf':
                driver_for_access_urls = 'HTTPServer'
            url = ds.access_urls[driver_for_access_urls]
            if 'cache' in self.metadata.keys():
                #print('access_urls add simplecache')
                url = f'{self.metadata["cache"]}{url}'
            #else:
                #print('access_urls dont add simplecache')
            return url

        #print('self.driver',self.driver, 'self.cache', self.cache)
        self._entries.update(
            {
                ds.name: LocalCatalogEntry(
                    ds.name,
                    'THREDDS data',
                    self.driver,
                    True,
                    {'urlpath': access_urls(ds, self), 'chunks': None},
                    [],
                    [],
                    {},
                    None,
                    catalog=self,
                )
                for ds in self.cat.datasets.values()
            }
        )

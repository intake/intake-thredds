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

        # data entries (only those with opendap links)
        if self.driver == 'opendap':
            driver_for_access_urls = 'OPENDAP'
        elif self.driver == 'netcdf':
            driver_for_access_urls = 'HTTPServer'
        self._entries.update(
            {
                ds.name: LocalCatalogEntry(
                    ds.name,
                    'THREDDS data',
                    self.driver,
                    True,
                    {'urlpath': ds.access_urls[driver_for_access_urls], 'chunks': None},
                    [],
                    [],
                    {},
                    None,
                    catalog=self,
                )
                for ds in self.cat.datasets.values()
            }
        )

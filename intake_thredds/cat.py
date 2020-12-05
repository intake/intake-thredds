from intake.catalog import Catalog
from intake.catalog.local import LocalCatalogEntry


class ThreddsCatalog(Catalog):
    name = 'thredds_cat'

    def __init__(self, url, metadata=None):
        self.url = url
        super().__init__(metadata=metadata)

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
        self._entries.update(
            {
                ds.name: LocalCatalogEntry(
                    ds.name,
                    'THREDDS data',
                    # 'netcdf',
                    'opendap',
                    True,
                    # {'urlpath': ds.access_urls['HTTPServer'], 'chunks': None},
                    {'urlpath': ds.access_urls['OPENDAP'], 'chunks': None},
                    [],
                    [],
                    {},
                    None,
                    catalog=self,
                )
                for ds in self.cat.datasets.values()
            }
        )

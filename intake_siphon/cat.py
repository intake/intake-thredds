from intake.catalog import Catalog
from intake.catalog.local import LocalCatalogEntry


class SiphonCatalog(Catalog):
    name = 'thredds_cat'

    def __init__(self, url, metadata=None, **kwargs):
        self.url = url
        super().__init__(metadata)

    def _load(self):
        from siphon.catalog import TDSCatalog
        self.cat = TDSCatalog(self.url, **self.kwargs)
        self.name = self.cat.catalog_name
        self._entries = {
            r.title: LocalCatalogEntry(
                r.title, 'THREDDS cat', 'thredds_cat', True, {'url': r.href},
                [], [], {}, None, catalog=self)
            for r in self.cat.catalog_refs.values()
        }
        self._entries.update({
            ds.name: LocalCatalogEntry(
                ds.name, 'THREDDS data', 'netcdf', True,
                {'urlpath': ds.access_urls['OPENDAP'], 'chunks': None},
                [], [], {}, None, catalog=self)
            for ds in self.cat.datasets.values()
        })

#(self, name, description, driver, direct_access, args, cache,
#                 parameters, metadata, catalog_dir, getenv=True,
#                 getshell=True, catalog=None)

import intake
intake.registry['thredds_cat'] = SiphonCatalog

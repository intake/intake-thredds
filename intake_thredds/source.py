import fnmatch
from intake_xarray import NetCDFSource
from intake_xarray.base import DataSourceMixin
from .cat import ThreddsCatalog


class THREDDSMergedSource(DataSourceMixin):

    def __init__(self, url, path, metadata=None):
        """

        Parameters
        ----------
        url : str
            Location of server
        path : list of str
            Subcats to follow; include glob characters (*, ?) in here for
            matching
        metadata : dict or None
            To associate with this source
        """
        super(THREDDSMergedSource, self).__init__(metadata)
        self.urlpath = url
        self.path = path
        self._ds = None

    def _open_dataset(self):
        import xarray
        if self._ds is None:
            cat = ThreddsCatalog(self.urlpath)
            for i in range(len(self.path)):
                part = self.path[i]
                if "*" not in part and "?" not in part:
                    cat = cat[part]()
                else:
                    break
            path = self.path[i:]
            data = [ds.to_dask() for ds in match(cat, path)]

            self._ds = xarray.auto_combine(data)


def match(cat, patterns):
    out = []
    for name in cat:
        if fnmatch.fnmatch(name, patterns[0]):
            if len(patterns) == 1:
                out.append(cat[name]())
            else:
                out.extend(match(cat[name](), patterns[1:]))
    return out

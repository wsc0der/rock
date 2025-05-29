"""
EM Configuration Module
"""
from pathlib import Path
import json
from rock.config import ROOT_DIR

class EMCache:
    """
    EMCache is a class that manages the cache for search results in the EM (East Money) application.
    """

    def __init__(self):
        """
        Initialize the EMCache with an empty search result cache.
        """
        super().__init__()

        self._search_result = {}
        path = Path(self.search_result_path)

        if path.exists():
            load_success = False
            with path.open("r", encoding="utf-8") as f:
                try:
                    self._search_result = json.load(f)
                    load_success = True
                except ValueError:
                    pass
            if not load_success:
                with path.open("w", encoding="utf-8"):
                    pass

    @property
    def search_result_path(self):
        """
        Returns the path to the search result cache file.
        """
        return str(ROOT_DIR/"search-cach.json")

    @property
    def search_result(self):
        """
        Returns the cached search results.
        """
        return self._search_result


em_cache = EMCache()

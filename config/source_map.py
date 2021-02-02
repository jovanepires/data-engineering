import re
import yaml

from config.config_map import ConfigMap


class SourceMap(ConfigMap):
    def __init__(self, file_name):
        super().__init__(file_name=file_name)

    def get(self, source, column):
        """
        return:
        """
        for k in self._data["map"]:
            if k.get("source", "") == source:
                columns = k.get("columns", [])
                for c in columns:
                    if c.get("from", "") == column:
                        return c.get("to", "")

        return re.sub(r"(?<!^)(?=[A-Z])", "_", column).lower()

    def list(self): 
        for k in self._data["map"]:
            yield k.get("source", "")

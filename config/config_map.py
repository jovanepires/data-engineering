import re
import yaml

class ConfigMap():
  def __init__(self, file_name):
    self._data = yaml.load(open(file_name), Loader=yaml.FullLoader)
  
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
    
    return re.sub(r'(?<!^)(?=[A-Z])', '_', column).lower()
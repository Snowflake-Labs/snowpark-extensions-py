from snowflake.snowpark import Column
from snowflake.snowpark.functions import get, lit

if not hasattr(Column,"___extended"):
    Column.__extended = True
    def getItem(self, index):
        name = str(self._expression).replace("\"",'') + f"[{index}]"
        return get(self,lit(index)).alias(name)

    Column.getItem = getItem


from snowflake.snowpark import DataFrame, Row, DataFrameReader
from snowflake.snowpark.types import StructType
from snowflake.snowpark import context
from typing import Any, Union, List, Optional
from snowflake.snowpark.functions import lit
from snowflake.snowpark.dataframe import _generate_prefix

if not hasattr(DataFrameReader,"___extended"):
    
    DataFrameReader.___extended = True
    DataFrameReader.__option = DataFrameReader.option
    def _option(self, key: str, value: Any) -> "DataFrameReader":
        key = key.upper()
        if key == "SEP":
            key = "FIELD_DELIMITER"
        elif key == "HEADER":
            key  ="SKIP_HEADER"
            value = 1 if value == True or str(value).upper() == "TRUE" else 0
        self.__option(key,value)

    def _load(self,path: Union[str, List[str], None] = None, format: Optional[str] = None, schema: Union[StructType, str, None] = None,stage=None, **options) -> "DataFrame":
        self.options(dict(options))
        self.format(format)
        if schema:
            self.schema(schema)
        files = []
        if isinstance(path,list):
            files.extend(path)
        else:
            files.append(path)
        session = context.get_active_session()
        if stage is None:
            stage = f'{session.get_fully_qualified_current_schema()}.{_generate_prefix("TEMP_STAGE")}'
            session.sql(f'create TEMPORARY stage if not exists {stage}').show()
        stage_files = [x for x in path if x.startswith("@")]
        if len(stage_files) > 1:
            raise Exception("Currently only one staged file can be specified. You can use a pattern if you want to specify several files")
        print(f"Uploading files using stage {stage}")
        for file in files:
            if file.startswith("file://"): # upload local file
                session.file.put(file,stage)
            elif file.startswith("@"): #ignore it is on an stage
                return self._read_semi_structured_file(file,format)
            else: #assume it is file too
                session.file.put(f"file://{file}",f"@{stage}")
        if self._file_type == "csv":
            return self.csv(f"@{stage}")
        return self._read_semi_structured_file(f"@{stage}",format)

    def _format(self, file_type: str) -> "DataFrameReader":
        file_type = str(file_type).lower()
        if file_type in ["csv","json","avro","orc","parquet","xml"]:
            self._file_type = file_type
        else:
            raise Exception(f"Unsupported file format {file_type}")
    
    DataFrameReader.format = _format
    DataFrameReader.load   = _load
    DataFrameReader.option = _option
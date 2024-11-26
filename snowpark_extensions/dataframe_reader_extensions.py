from snowflake.snowpark import DataFrame, Row, DataFrameReader
from snowflake.snowpark.types import StructType
from snowflake.snowpark import context
from typing import Any, Union, List, Optional
from snowflake.snowpark.functions import lit
from snowflake.snowpark.dataframe import _generate_prefix

if not hasattr(DataFrameReader,"___extended"):
    import logging    
    DataFrameReader.___extended = True
    DataFrameReader.__option = DataFrameReader.option
    DataFrameReader.__csv = DataFrameReader.csv
    
    def _option(self, key: str, value: Any) -> "DataFrameReader":
        key = key.upper()
        if key == "SEP" or key == "DELIMITER":
            key = "FIELD_DELIMITER"
        elif key == "HEADER":
            key  ="SKIP_HEADER"
            value = 1 if value == True or str(value).upper() == "TRUE" else 0
        elif key == "LINESEP":
            key  ="RECORD_DELIMITER"
        elif key == "PATHGLOBFILTER":
            key = "PATTERN"
        elif key == "CODEC":
            key = "COMPRESSION"
        elif key == "QUOTE":
            key = "FIELD_OPTIONALLY_ENCLOSED_BY"
        elif key == "NULLVALUE":
            key = "NULL_IF"
        elif key == "DATEFORMAT":
            key = "DATE_FORMAT"
        elif key == "TIMESTAMPFORMAT":
            key = "TIMESTAMP_FORMAT"
        elif key == "INFERSCHEMA":
            key = "INFER_SCHEMA"
        elif key in ["RECURSIVEFILELOOKUP","QUOTEALL","MODIFIEDBEFORE","MODIFIEDAFTER","MULTILINE","MERGESCHEMA"]:
            logging.error(f"DataFrameReader option {key} is not supported")
            return self       
        return self.__option(key,value)

    def _load(self,path: Union[str, List[str], None] = None, format: Optional[str] = None, schema: Union[StructType, str, None] = None,stage=None, **options) -> "DataFrame":
        self.options(dict(options))
        if "INFER_SCHEMA" in options:
            if "SKIP_HEADER" in options:
                if options["SKIP_HEADER"] == 0:
                    options["PARSE_HEADER"] = True
                else:
                    options["PARSE_HEADER"] = False
                del options["SKIP_HEADER"]
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

    def _csv(self,
             path: str,
             schema: Optional[Union[StructType, str]] = None,
             sep: Optional[str] = None,
             encoding: Optional[str] = None,
             quote: Optional[str] = None,
             escape: Optional[str] = None,
             comment: Optional[str] = None,
             header: Optional[Union[bool, str]] = None,
             inferSchema: Optional[Union[bool, str]] = None,
             ignoreLeadingWhiteSpace: Optional[Union[bool, str]] = None,
             ignoreTrailingWhiteSpace: Optional[Union[bool, str]] = None,
             nullValue: Optional[str] = None,
             nanValue: Optional[str] = None,
             positiveInf: Optional[str] = None,
             negativeInf: Optional[str] = None,
             dateFormat: Optional[str] = None,
             timestampFormat: Optional[str] = None,
             maxColumns: Optional[Union[int, str]] = None,
             maxCharsPerColumn: Optional[Union[int, str]] = None,
             maxMalformedLogPerPartition: Optional[Union[int, str]] = None,
             mode: Optional[str] = None,
             columnNameOfCorruptRecord: Optional[str] = None,
             multiLine: Optional[Union[bool, str]] = None,
             charToEscapeQuoteEscaping: Optional[str] = None,
             samplingRatio: Optional[Union[float, str]] = None,
             enforceSchema: Optional[Union[bool, str]] = None,
             emptyValue: Optional[str] = None,
             locale: Optional[str] = None,
             lineSep: Optional[str] = None,
             pathGlobFilter: Optional[Union[bool, str]] = None,
             recursiveFileLookup: Optional[Union[bool, str]] = None,
             modifiedBefore: Optional[Union[bool, str]] = None,
             modifiedAfter: Optional[Union[bool, str]] = None,
             unescapedQuoteHandling: Optional[str] = None
             ) -> "DataFrame":
        params = {k: v for k, v in locals().items() if v is not None}
        params.pop("self", None)
        params.pop("path", None)
        params.pop("schema", None)
        if schema:
            self.schema(schema)
        for key, value in params.items():
          self = self.option(key, value)
        return self.__csv(path)

    DataFrameReader.format = _format
    DataFrameReader.load   = _load
    DataFrameReader.option = _option
    DataFrameReader.csv = _csv
    
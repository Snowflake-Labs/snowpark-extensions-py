from snowflake.snowpark.types import StructType, StructField

if not hasattr(StructType, "___extended"):
  StructType.___extended = True    
  def schema__get_item__(self,index):
      if isinstance(index, str):
         name = index.upper()
         for f in self.fields:
            if name == f.name.upper():
               return f
      else:
         return self.fields[index]
  StructType.__getitem__ = schema__get_item__
  StructField.dtype = property(lambda self: self.datatype)
  from snowflake.snowpark._internal.type_utils import convert_sf_to_sp_type
  def fromJson(cls, json: dict) -> StructField:
        return StructField(
            json["name"],
            convert_sf_to_sp_type(json["type"]),
            json.get("nullable", True),
            json.get("metadata"),
        )
  StructField.fromJson = fromJson
  StructType.fromJson =  lambda cls,json: StructType([StructField.fromJson(f) for f in json["fields"]])
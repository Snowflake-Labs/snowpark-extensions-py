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
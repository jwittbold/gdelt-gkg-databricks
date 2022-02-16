from pyspark.sql.types import StringType, IntegerType, DecimalType, LongType, \
    DateType, TimestampType, StructType, StructField, ArrayType, BooleanType, FloatType, DoubleType


GCAM_schema = StructType([    
    StructField('Variable', StringType(), True),            # 0
    StructField('DictionaryID', IntegerType(), True),       # 1
    StructField('DimensionID', IntegerType(), True),        # 2
    StructField('ValueType', StringType(), True),           # 3
    StructField('LanguageCode', StringType(), True),        # 4
    StructField('DictionaryHumanName', StringType(), True), # 5
    StructField('DimensionHumanName', StringType(), True),  # 6
    StructField('DictionaryCitation', StringType(), True)   # 7
])
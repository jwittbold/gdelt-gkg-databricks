from pyspark.sql.types import StringType, IntegerType, DecimalType, LongType, \
     DateType, TimestampType, StructType, StructField, ArrayType, BooleanType, FloatType, DoubleType


url_schema = StructType([    

    # StructField('url_prfix', StringType(), True),
    StructField('numeric_date_time', LongType(), True),
    StructField('url_suffix', StringType(), True),
    StructField('gkg_url', StringType(), True)
])
from pyspark.sql.types import StringType, IntegerType, DecimalType, LongType, \
                            DateType, TimestampType, StructType, StructField, \
                            ArrayType, BooleanType, Row, DoubleType

metrics_schema = StructType([    
    StructField('file_name', StringType(), True),
    StructField('gkg_record_id', StringType(), True),
    StructField('gkg_timestamp', TimestampType(), True),
    StructField('translingual', BooleanType(), True),
    StructField('csv_size_mb', DecimalType(precision=8, scale=6), True),
    StructField('local_download_time', TimestampType(), True),
])
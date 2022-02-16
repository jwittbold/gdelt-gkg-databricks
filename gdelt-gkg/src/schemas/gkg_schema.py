from pyspark.sql.types import StringType, IntegerType, DecimalType, LongType, \
     DateType, TimestampType, StructType, StructField, ArrayType, BooleanType, FloatType, DoubleType


# Schema for GKG 2.0
# GDELT 2.0 Global Knowledge Graph Codebook (V2.1)
# http://data.gdeltproject.org/documentation/GDELT-Global_Knowledge_Graph_Codebook-V2.1.pdf

# 0
GkgRecordIdStruct = StructType([
    StructField('Date', LongType(), True),
    StructField('Translingual', BooleanType(), True),
    StructField('NumberInBatch', IntegerType(), True)
])

# 1
V21DateStruct = StructType([StructField('V21Date', TimestampType(), True)])

# 2 
V2SrcCollectionIdStruct = StructType([StructField('V2SrcCollectionId', StringType(), True)])

# 3 
V2SrcCmnNameStruct  = StructType([StructField('V2SrcCmnName', StringType(), True)])

# 4
V2DocIdStruct = StructType([StructField('V2DocId', StringType(), True)])

# 5
V1CountStruct = ArrayType(StructType([
    StructField('CountType', StringType(), True),
    StructField('Count', LongType(), True),
    StructField('ObjectType', StringType(), True),
    StructField('LocationType', IntegerType(), True),
    StructField('FullName', StringType(), True),
    StructField('CountryCode', StringType(), True),
    StructField('ADM1Code', StringType(), True),
    StructField('LocationLatitude', DecimalType(9, 7), True),
    StructField('LocationLongitude', DecimalType(10, 7), True),
    StructField('FeatureId', StringType(), True),
]))

# 6
V21CountStruct = ArrayType(StructType([
    StructField('CountType', StringType(), True),
    StructField('Count', LongType(), True),
    StructField('ObjectType', StringType(), True),
    StructField('LocationType', IntegerType(), True),
    StructField('FullName', StringType(), True),
    StructField('CountryCode', StringType(), True),
    StructField('ADM1Code', StringType(), True),
    StructField('LocationLatitude', DecimalType(9, 7), True),
    StructField('LocationLongitude', DecimalType(10, 7), True),
    StructField('FeatureId', StringType(), True),
    StructField('CharOffset', IntegerType(), True)
]))

# 7
V1ThemeStruct = StructType([StructField('V1Theme', ArrayType(StringType()), True)])

# 8
V2EnhancedThemeStruct = ArrayType(StructType([
    StructField('V2Theme', StringType(), True),
    StructField('CharOffset', IntegerType(), True)
]))

# 9
V1LocationStruct = ArrayType(StructType([
    StructField('LocationType', IntegerType(), True),
    StructField('FullName', StringType(), True),
    StructField('CountryCode', StringType(), True),
    StructField('ADM1Code', StringType(), True),
    StructField('LocationLatitude', DecimalType(9, 7), True),
    StructField('LocationLongitude', DecimalType(10, 7), True),
    StructField('FeatureId', StringType(), True),
]))

# 10
V2EnhancedLocationStruct = ArrayType(StructType([
    StructField('LocationType', IntegerType(), True),
    StructField('FullName', StringType(), True),
    StructField('CountryCode', StringType(), True),
    StructField('ADM1Code', StringType(), True),
    StructField('ADM2Code', StringType(), True),
    StructField('LocationLatitude', DecimalType(9, 7), True),
    StructField('LocationLongitude', DecimalType(10, 7), True),
    StructField('FeatureId', StringType(), True),
    StructField('CharOffset', IntegerType(), True)
]))

# 11
V1PersonStruct = StructType([
    StructField('V1Person', ArrayType(StringType()), True)
])

# 12
V2EnhancedPersonStruct = ArrayType(StructType([
    StructField('V1Person', StringType(), True),
    StructField('CharOffset', IntegerType(), True)
]))

# 13
V1OrgsStruct = StructType([StructField('V1Org', ArrayType(StringType()), True)])

# 14
V2EnhancedOrgStruct = ArrayType(StructType([
    StructField('V1Org', StringType(), True),
    StructField('CharOffset', IntegerType(), True)
]))

# 15
V15ToneStruct = StructType([
    StructField('Tone', DoubleType(), True),
    StructField('PositiveScore', DoubleType(), True),
    StructField('NegativeScore', DoubleType(), True),
    StructField('Polarity', DoubleType(), True),
    StructField('ActivityRefDensity', DoubleType(), True),
    StructField('SelfGroupRefDensity', DoubleType(), True),
    StructField('WordCount', IntegerType(), True)
])

# 16
V21EnhancedDateStruct = ArrayType(StructType([
    StructField('DateResolution', IntegerType(), True),
    StructField('Month', IntegerType(), True),
    StructField('Day', IntegerType(), True),
    StructField('Year', IntegerType(), True),
    StructField('CharOffset', IntegerType(), True),
]))

# 17
V2GcamStruct = ArrayType(StructType([
    StructField('DictionaryDimId', StringType(), True),
    StructField('Score', DoubleType(), True)
]))

# 18
V21ShareImgStruct = StructType([
    StructField('V21ShareImg', StringType(), True)
])

# 19
V21RelImgStruct = StructType([
    StructField('V21RelImg', ArrayType(StringType()), True)
])

# 20
V21SocImageStruct = StructType([
    StructField('V21SocImage', ArrayType(StringType()), True)
])

# 21
V21SocVideoStruct = StructType([
    StructField('V21SocVideo', ArrayType(StringType()), True)
])

# 22
V21QuotationStruct = ArrayType(StructType([
    StructField('Offset', IntegerType(), True),
    StructField('CharLength', IntegerType(), True),
    StructField('Verb', StringType(), True),
    StructField('Quote', StringType(), True)
]))

# 23
V21NameStruct = ArrayType(StructType([
    StructField('Name', StringType(), True),
    StructField('CharOffset', IntegerType(), True)
]))

# 24
V21AmountStruct = ArrayType(StructType([
    StructField('Amount', DoubleType(), True),
    StructField('Object', StringType(), True),
    StructField('Offset', IntegerType(), True)
]))

# 25 
V21TranslationInfoStruct = StructType([
    StructField('Srclc', StringType(), True),
    StructField('Eng', StringType(), True)
])

# 26
V2ExtrasXMLStruct = StructType([
    StructField('Title', StringType(), True),
    StructField('Author', StringType(), True),
    StructField('Links', StringType(), True),
    StructField('AltUrl', StringType(), True),
    StructField('AltUrlAmp', StringType(), True),
    StructField('PubTimestamp', TimestampType(), True),
])


# Schema collects all structs
gkg_schema = StructType([
                                                    
    StructField('GkgRecordId',          GkgRecordIdStruct, True),           # 0
    StructField('V21Date',              V21DateStruct, True),               # 1
    StructField('V2SrcCollectionId',    V2SrcCollectionIdStruct, True),     # 2
    StructField('V2SrcCmnName',         V2SrcCmnNameStruct, True),          # 3
    StructField('V2DocId',              V2DocIdStruct, True),               # 4
    StructField('V1Counts',             V1CountStruct, True),               # 5
    StructField('V21Counts',            V21CountStruct, True),              # 6
    StructField('V1Themes',             V1ThemeStruct, True),               # 7 
    StructField('V2EnhancedThemes',     V2EnhancedThemeStruct, True),       # 8
    StructField('V1Locations',          V1LocationStruct, True),            # 9
    StructField('V2Locations',          V2EnhancedLocationStruct, True),    # 10
    StructField('V1Persons',            V1PersonStruct, True),              # 11
    StructField('V2Persons',            V2EnhancedPersonStruct, True),      # 12
    StructField('V1Orgs',               V1OrgsStruct, True),                # 13
    StructField('V2Orgs',               V2EnhancedOrgStruct, True),         # 14
    StructField('V15Tone',              V15ToneStruct, True),               # 15
    StructField('V21EnhancedDates',     V21EnhancedDateStruct, True),       # 16
    StructField('V2GCAM',               V2GcamStruct, True),                # 17
    StructField('V21ShareImg',          V21ShareImgStruct, True),           # 18
    StructField('V21RelImg',            V21RelImgStruct, True),             # 19
    StructField('V21SocImage',          V21SocImageStruct, True),           # 20
    StructField('V21SocVideo',          V21SocVideoStruct, True),           # 21
    StructField('V21Quotations',        V21QuotationStruct, True),          # 22
    StructField('V21AllNames',          V21NameStruct, True),               # 23
    StructField('V21Amounts',           V21AmountStruct, True),             # 24
    StructField('V21TransInfo',         V21TranslationInfoStruct, True),    # 25
    StructField('V2ExtrasXML',          V2ExtrasXMLStruct, True)            # 26
])
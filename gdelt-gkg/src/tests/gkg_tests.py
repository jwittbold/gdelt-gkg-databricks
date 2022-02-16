from pyspark.sql import SparkSession 
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


from schemas.gkg_schema import gkg_schema
from etl.parse_gkg import gkg_parser


spark = SparkSession \
    .builder \
    .master('local[*]') \
    .appName('parse_gkg_test') \
    .getOrCreate()



# data_file = 'file:///Users/jackwittbold/Desktop/gdelt_data/py_arrow_test/20210401001500.gkg.csv'
data_file = '/test/20210401001500.gkg.csv'

gkg_translation_file = '/test/20210909034500.translation.gkg.csv'
# '/test/20210401010000.translation.gkg.csv'


raw_gkg = spark.sparkContext.textFile(gkg_translation_file)
parsed = raw_gkg.map(lambda Row: gkg_parser(Row))




df = spark.createDataFrame(parsed, schema=gkg_schema)
df.printSchema()
# df.count()
# df.show()

print(df.columns)


# df.write.mode('overwrite').parquet('/test_out/pyspark_out_test/_0.parquet')
# parquet('/Users/jackwittbold/Desktop/gdelt_data/unicode_version.parquet')


#  quote_final_partitioned.write.mode('overwrite').parquet()

# df.select('V1Persons.V1Person').filter('V1Persons.V1Person IS NOT null').show(truncate=False)
# df.select('V21EnhancedDates.year').filter('V21EnhancedDates.year IS NOT null').show(truncate=False)

# df.select('V21Quotations.Quote').filter('V21Quotations.Quote IS NOT null').show(truncate=False)




df.select('GkgRecordId').show(truncate=False)                                                             # looks OK
# df.select('GkgRecordId.*').show(truncate=False)                                                           # looks OK broken out *
# df.select('V21Date').show(truncate=False)                                                                 # looks OK
# df.select('V2SrcCollectionId').show()                                                                     # looks OK - Rows
# df.select('V2SrcCmnName').show(truncate=False)                                                            # looks OK - Rows
# df.select('V2DocId').show(truncate=False)                                                                 # looks OK - Rows

# #########################################################

# df.select('V1Counts').filter('V1Counts IS NOT null').show()                                               # looks OK - list
# df.select('V21Counts.CountType').filter('V21Counts.CountType IS NOT null').show(truncate=False)             # looks OK - list
# df.select('V1Themes').filter('V1Themes IS NOT null').show()                                                   # looks OK - 
# df.select('V2EnhancedThemes').filter('V2EnhancedThemes IS NOT null').show()                                   # OK
# df.select('V2EnhancedThemes.V2Theme').filter('V2EnhancedThemes.V2Theme IS NOT null').show()                   # OK
# df.select('V1Locations').filter('V1Locations IS NOT null').show()                                             # OK
# df.select('V2Locations').filter('V2Locations IS NOT null').show()                                             # OK

#########################################################

# df.select('V1Persons').filter('V1Persons IS NOT null').show(truncate=False)                                 # revisit - ok i think
# df.select('V2Persons.V1Person').filter('V2Persons.V1Person IS NOT null').show(truncate=False)               # revisit - ok i think
# df.select('V1Persons').show(truncate=False)
# df.select('V2Persons').show(truncate=False)

# df.select('V1Orgs').filter('V1Orgs IS NOT null').show()                                                     # OK
# df.select('V2Orgs').filter('V2Orgs IS NOT null').show()                                                     # OK
# df.select('V15tone').show(truncate=False)                                                                   # OK

#########################################################

# df.select('V21EnhancedDates').show(truncate=False)                                                          # OK
# df.select('V2GCAM').show(1, truncate=False)                                                                 # OK
# df.select('V21ShareImg').show(truncate=False)                                                               # OK
# df.select('V21RelImg').filter('V21RelImg IS NOT null').show()                                               # OK                         
# df.select('V21SocImage').filter('V21SocImage IS NOT null').show()                                           # OK
# df.select('V21SocVideo').show(truncate=False)                                                               # OK

#########################################################

# df.select('V21Quotations').filter('V21Quotations IS NOT null').show()                                       # OK
# df.select('V21AllNames').show()                                                                             # OK
# df.select('V21Amounts').show()                                                                              # OK
# df.select('V21TransInfo').show(truncate=False)                                                              # OK
# df.select('V2ExtrasXML').show(truncate=False)                                                               # OK




# not working
# unicode_test = '/Users/jackwittbold/Desktop/gdelt_data/unicode_version.parquet'
# unicode_df = spark.read.parquet(unicode_test)
# unicode_df.show()


# df.show()

# df.createOrReplaceTempView('gkg_file')

# anal_query = spark.sql("""
#                 SELECT GkgRecordId, V2DocId, V2Persons, V15Tone.Tone
#                  FROM gkg_file
#                  WHERE array_contains(V2GCAM.DictionaryDimId, 'c8.19')
#                  GROUP BY GkgRecordId, V2DocId, V2Persons, V21ShareImg, V15Tone.Tone
#                  ORDER BY V15Tone.Tone ASC;
#                  """)

# anal_query.show(truncate=False)

# gcam_query = spark.sql("""
                # SELECT V2GCAM.DictionaryDimId, MAX(V2GCAM.Score) AS highest_score 
                #  FROM gkg_file
                #  WHERE 
                #  GROUP BY V2GCAM.DictionaryDimId
                #  ORDER BY highest_score DESC
                #  """)

# gcam_query.show(1, truncate=False)





# query = spark.sql("""
#                 SELECT GkgRecordId.NumberInBatch, V2DocId, V1Persons, V2Persons
#                 FROM gkg_file
#                 WHERE V1Persons.V1Person = 'kostadin angelov'
#                 """)

# WHERE V2Persons.V1Person = 'Sheinelle Jones'
# WHERE V1Persons.V1Person IN ('katrina waghorne')

# query.show(truncate=False)


# another_query = spark.sql("""
#                         SELECT GkgRecordId.NumberInBatch, V2DocId, V1Counts, V21Counts
#                         FROM gkg_file
#                         WHERE array_contains(V1Counts.FullName, 'Alamance County, North Carolina, United States')
#                             """)


# # WHERE V1Counts.FullName = 'Alamance County, North Carolina, United States'
# another_query.show()


#WORKS
# query_3 = spark.sql("""
#                         SELECT GkgRecordId.NumberInBatch, V2DocId, V1Themes, V2EnhancedThemes
#                         FROM gkg_file
#                         WHERE array_contains(V1Themes.V1Theme, 'EPU_CATS_NATIONAL_SECURITY')
#                         """)

# query_3.show()

# WORKS
# query_4 = spark.sql("""
#                         SELECT GkgRecordId.NumberInBatch, V2DocId, V1Themes, V2EnhancedThemes
#                         FROM gkg_file
#                         WHERE array_contains(V2EnhancedThemes.V2Theme, 'WB_162_TRANSPORT_ECONOMICS')
#                         """)

# query_4.show()


# WORKS
# query_5 = spark.sql("""
#                         SELECT GkgRecordId.NumberInBatch, V2DocId, V1Orgs, V2Orgs
#                         FROM gkg_file
#                         WHERE array_contains(V1Orgs.V1Org, 'national coronavirus task')
#                         """)

# query_5.show(truncate=False)


# WORKING
# query_5_5 = spark.sql("""
#                         SELECT GkgRecordId.NumberInBatch, V2DocId, V1Orgs, V2Orgs.V1Org
#                         FROM gkg_file
#                         WHERE array_contains(V2Orgs.V1Org, 'National Vaccination Task')
#                         """)

# query_5_5.show(truncate=False)

# https://jaceklaskowski.gitbooks.io/mastering-spark-sql/content/spark-sql-functions-collection.html
# https://medium.com/expedia-group-tech/deep-dive-into-apache-spark-array-functions-720b8fbfa729



# query_6 = spark.sql("""
#                         SELECT GkgRecordId.NumberInBatch, V2DocId, V1Locations, V2Locations
#                         FROM gkg_file
#                         WHERE array_contains(V2Locations.FullName, 'Taiwan')
#                         """)

# query_6.show()



# query_7 = spark.sql("""
#                         SELECT GkgRecordId.NumberInBatch, V2DocId, V1Persons, V2Persons
#                         FROM gkg_file
#                         WHERE array_contains(V1Persons.V1Person, 'leonard cohen')
#                         """)

# query_7.show()


# WORKING
# query_8 = spark.sql("""
#                         SELECT GkgRecordId.NumberInBatch, V2DocId, V15Tone.Tone, V15Tone.Polarity, V21AllNames.Name
#                         FROM gkg_file
#                         WHERE array_contains(V2Locations.FullName, 'China') AND array_contains(V2Locations.FullName, 'Taiwan') AND array_contains(V21AllNames.Name, 'Meng Wanzhou')
#                         ORDER BY V15Tone.Polarity DESC;
#                         """)

# query_8.show(truncate=False)

# WORKING
# query_9 = spark.sql("""
#                         SELECT GkgRecordId.NumberInBatch, V2DocId, V15Tone.Tone, V15Tone.Polarity, V21AllNames.Name
#                         FROM gkg_file
#                         WHERE array_contains(V2Locations.FullName, 'China') AND array_contains(V21AllNames.Name, 'Universal Basic Income')
#                         ORDER BY V15Tone.Polarity DESC;
#                         """)

# query_9.show(truncate=False)






#### THIS WORKS BUT ONLY AFTER CREATING ARRAY FOR SINGLE ELEMENT AS SEEN WITHIN V21ShareImg func def  #########
#  # WHERE V21ShareImg = 'https://www.clactonandfrintongazette.co.uk/resources/images/12464265/'  Querying this way does not work on structtype 


# THIS WORKS - MUST DECIDE WHICH STRUCT TYPE MAKES MORE SENSE, SEEMINGLY THIS FIELD ONLY EVER CONTAINS ONE ITEM 
# query_10 = spark.sql("""
#                         SELECT GkgRecordId.NumberInBatch, V2DocId, V21ShareImg 
#                         FROM gkg_file
#                         WHERE V21ShareImg.V21ShareImg = 'https://www.clactonandfrintongazette.co.uk/resources/images/12464265/'
#                         ORDER BY V15Tone.Polarity DESC;
#                         """)

# query_10.show(truncate=False)


# WORKS
# query_10 = spark.sql("""
#                         SELECT GkgRecordId.NumberInBatch, V2DocId, V21RelImg 
#                         FROM gkg_file
#                         WHERE array_contains(V21RelImg.V21RelImg, 'https://content.api.news/v3/images/bin/7c9ec07956bf7d575b8d8bd0f5692b59')
#                         ORDER BY V15Tone.Polarity DESC;
#                         """)

# query_10.show(truncate=False)



# WORKING
# query_11 = spark.sql("""
#                         SELECT GkgRecordId.NumberInBatch, V2DocId, V21SocImage 
#                         FROM gkg_file
#                         WHERE array_contains(V21SocImage.V21SocImage, 'https://pic.twitter.com/7N6nssSHxR')
#                         ORDER BY V15Tone.Polarity DESC;
#                         """)

# query_11.show(truncate=False)



# WORKING
# query_12 = spark.sql("""
#                         SELECT GkgRecordId.NumberInBatch, V2DocId, V21SocVideo 
#                         FROM gkg_file
#                         WHERE array_contains(V21SocVideo.V21SocVideo, 'https://youtube.com/embed/eNKHEIrtIVY')
#                         ORDER BY V15Tone.Polarity DESC;
#                         """)

# query_12.show(truncate=False)


# WORKS
# query_13 = spark.sql("""
#                         SELECT GkgRecordId.NumberInBatch, V2DocId, V21Quotations 
#                         FROM gkg_file
#                         WHERE array_contains(V21Quotations.Quote, 'almost all of the ground lost in the labour market due to the pandemic will be made up by the end of 2021 -22')
#                         ORDER BY V15Tone.Polarity DESC;
#                         """)

# query_13.show(truncate=False)



# 1190|110||almost all of the ground lost in the labour market due to the pandemic will be made up by the end of 2021 -22


# WORKS
# query_13 = spark.sql("""
#                         SELECT GkgRecordId.NumberInBatch, V2DocId, V21Quotations 
#                         FROM gkg_file
#                         WHERE array_contains(V21Quotations.Quote, "We are honored that Brian Kelley chose WMN as home to his solo endeavors. I've been an FGL believer from day one , and BK clear vision for himself and his upcoming project is a testament to his heart , soul , and talent. His new music is the perfect antidote to 2020 , and what an incredible way to start 2021 !")

#                         ORDER BY V15Tone.Polarity DESC;
#                         """)

# query_13.show(truncate=False)


# WORKS
# query_14 = spark.sql("""
#                         SELECT GkgRecordId.NumberInBatch, V2DocId, V21Quotations 
#                         FROM gkg_file
#                         WHERE array_contains(V21Quotations.Quote, "Please! Please! x2026 ; I can't breathe!")

#                         ORDER BY V15Tone.Polarity DESC;
#                         """)

# query_14.show(truncate=False)



# WORKS
# query_15 = spark.sql("""
#                         SELECT GkgRecordId.NumberInBatch, V2DocId, V21AllNames
#                         FROM gkg_file
#                         WHERE array_contains(V21AllNames.Name, 'Paul Carcaterra')

#                         ORDER BY V15Tone.Polarity DESC;
#                         """)

# query_15.show(truncate=False)



# WORKS
# query_16 = spark.sql("""
#                         SELECT GkgRecordId.NumberInBatch, V2DocId, V21Amounts.Object, V21Amounts.Amount
#                         FROM gkg_file
                        
#                         WHERE array_contains(V21Amounts.Object, 'additional white metal specimens')

#                         ORDER BY V15Tone.Polarity DESC;
#                         """)

# query_16.show(truncate=False)

# 2,specimens were known,905;2,additional white metal specimens,950;2,coins were authenticated,1033;2,virtually identical specimens for,1442;5000000,past auction records with,2608;


# WORKS
# query_17 = spark.sql("""
#                         SELECT GkgRecordId.NumberInBatch, V2DocId, explode(V21Amounts.Object), V21Amounts.Amount
#                         FROM gkg_file
                        
#                         WHERE array_contains(V21Amounts.Object, 'additional white metal specimens')

#                         ORDER BY V15Tone.Polarity DESC;
#                         """)

# query_17.show(truncate=False)


# WORKING



# WORKING 
# query_18 = spark.sql("""
#                         SELECT GkgRecordId.NumberInBatch, V2DocId, V21TransInfo, V21TransInfo.Srclc, V21TransInfo.Eng
#                         FROM gkg_file
#                         WHERE V21TransInfo.Srclc IS NOT null
#                         """)

# query_18.show(truncate=False)




# WORKS
# query_19 = spark.sql("""
#                         SELECT GkgRecordId.NumberInBatch, V2DocId, V2ExtrasXML
#                         FROM gkg_file
#                         WHERE V2ExtrasXML.Author = 'Annie Werner'
#                         """)

# query_19.show(truncate=False)


# query_20 = spark.sql("""
#                         SELECT GkgRecordId.NumberInBatch, V2DocId, V2ExtrasXML
#                         FROM gkg_file

#                         """)

# query_20.show(truncate=False)



# query_20 = spark.sql("""
#                         SELECT GkgRecordId.NumberInBatch, V2DocId, V2GCAM
#                         FROM gkg_file

#                         """)

# query_20.show(1)


# query_21 = spark.sql("""
#                         SELECT GkgRecordId, V21Date, V2SrcCollectionId, V2SrcCmnName, V2DocId
#                         FROM gkg_file

#                         """)

# query_21.show(1, truncate=False)



# WORKS
# query_22 = spark.sql("""
#                         SELECT GkgRecordId.NumberInBatch, V2DocId, V1Themes, V2EnhancedThemes
#                         FROM gkg_file
#                         WHERE array_contains(V2EnhancedThemes.V2Theme, 'WB_162_TRANSPORT_ECONOMICS')
#                         """)

# query_22.show()


# df.show()

# df.summary().show()

# print(df.count())

# COLUMN NAMES
# 'GkgRecordId', 'V21Date', 'V2SrcCollectionId', 'V2SrcCmnName', 'V2DocId', 'V1Counts', 'V21Counts', 
# 'V1Themes', 'V2EnhancedThemes', 'V1Locations', 'V2Locations', 'V1Persons', 'V2Persons', 'V1Orgs', 
# 'V2Orgs', 'V15tone', 'V21EnhancedDates', 'V2GCAM', 'V21ShareImg', 'V21RelImg', 'V21SocImage', 
# 'V21SocVideo', 'V21Quotations', 'V21AllNames', 'V21Amounts', 'V21TransInfo', 'V2ExtrasXML']

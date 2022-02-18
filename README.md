# GDELT Global Knowlegde Graph Pipeline
***Global Database of Events, Language, and Tone***


## Running on Databricks cluster and Azure ADLS Gen 2 Storage via Databricks Connect


### Deployment Architecture
![deployment_architecture](/diagrams/gkg_pipeline_deployment_architecture_sm.png)


### Azure Dashboard
![gdelt_pipeline_dashboard](/screenshots/gdelt_pipeline_dashboard.png)


## GDELT GKG Schema
```
root
 |-- GkgRecordId: struct (nullable = true)
 |    |-- Date: long (nullable = true)
 |    |-- Translingual: boolean (nullable = true)
 |    |-- NumberInBatch: integer (nullable = true)
 |-- V21Date: struct (nullable = true)
 |    |-- V21Date: timestamp (nullable = true)
 |-- V2SrcCollectionId: struct (nullable = true)
 |    |-- V2SrcCollectionId: string (nullable = true)
 |-- V2SrcCmnName: struct (nullable = true)
 |    |-- V2SrcCmnName: string (nullable = true)
 |-- V2DocId: struct (nullable = true)
 |    |-- V2DocId: string (nullable = true)
 |-- V1Counts: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- CountType: string (nullable = true)
 |    |    |-- Count: long (nullable = true)
 |    |    |-- ObjectType: string (nullable = true)
 |    |    |-- LocationType: integer (nullable = true)
 |    |    |-- FullName: string (nullable = true)
 |    |    |-- CountryCode: string (nullable = true)
 |    |    |-- ADM1Code: string (nullable = true)
 |    |    |-- LocationLatitude: decimal(9,7) (nullable = true)
 |    |    |-- LocationLongitude: decimal(10,7) (nullable = true)
 |    |    |-- FeatureId: string (nullable = true)
 |-- V21Counts: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- CountType: string (nullable = true)
 |    |    |-- Count: long (nullable = true)
 |    |    |-- ObjectType: string (nullable = true)
 |    |    |-- LocationType: integer (nullable = true)
 |    |    |-- FullName: string (nullable = true)
 |    |    |-- CountryCode: string (nullable = true)
 |    |    |-- ADM1Code: string (nullable = true)
 |    |    |-- LocationLatitude: decimal(9,7) (nullable = true)
 |    |    |-- LocationLongitude: decimal(10,7) (nullable = true)
 |    |    |-- FeatureId: string (nullable = true)
 |    |    |-- CharOffset: integer (nullable = true)
 |-- V1Themes: struct (nullable = true)
 |    |-- V1Theme: array (nullable = true)
 |    |    |-- element: string (containsNull = true)
 |-- V2EnhancedThemes: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- V2Theme: string (nullable = true)
 |    |    |-- CharOffset: integer (nullable = true)
 |-- V1Locations: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- LocationType: integer (nullable = true)
 |    |    |-- FullName: string (nullable = true)
 |    |    |-- CountryCode: string (nullable = true)
 |    |    |-- ADM1Code: string (nullable = true)
 |    |    |-- LocationLatitude: decimal(9,7) (nullable = true)
 |    |    |-- LocationLongitude: decimal(10,7) (nullable = true)
 |    |    |-- FeatureId: string (nullable = true)
 |-- V2Locations: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- LocationType: integer (nullable = true)
 |    |    |-- FullName: string (nullable = true)
 |    |    |-- CountryCode: string (nullable = true)
 |    |    |-- ADM1Code: string (nullable = true)
 |    |    |-- ADM2Code: string (nullable = true)
 |    |    |-- LocationLatitude: decimal(9,7) (nullable = true)
 |    |    |-- LocationLongitude: decimal(10,7) (nullable = true)
 |    |    |-- FeatureId: string (nullable = true)
 |    |    |-- CharOffset: integer (nullable = true)
 |-- V1Persons: struct (nullable = true)
 |    |-- V1Person: array (nullable = true)
 |    |    |-- element: string (containsNull = true)
 |-- V2Persons: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- V1Person: string (nullable = true)
 |    |    |-- CharOffset: integer (nullable = true)
 |-- V1Orgs: struct (nullable = true)
 |    |-- V1Org: array (nullable = true)
 |    |    |-- element: string (containsNull = true)
 |-- V2Orgs: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- V1Org: string (nullable = true)
 |    |    |-- CharOffset: integer (nullable = true)
 |-- V15Tone: struct (nullable = true)
 |    |-- Tone: double (nullable = true)
 |    |-- PositiveScore: double (nullable = true)
 |    |-- NegativeScore: double (nullable = true)
 |    |-- Polarity: double (nullable = true)
 |    |-- ActivityRefDensity: double (nullable = true)
 |    |-- SelfGroupRefDensity: double (nullable = true)
 |    |-- WordCount: integer (nullable = true)
 |-- V21EnhancedDates: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- DateResolution: integer (nullable = true)
 |    |    |-- Month: integer (nullable = true)
 |    |    |-- Day: integer (nullable = true)
 |    |    |-- Year: integer (nullable = true)
 |    |    |-- CharOffset: integer (nullable = true)
 |-- V2GCAM: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- DictionaryDimId: string (nullable = true)
 |    |    |-- Score: double (nullable = true)
 |-- V21ShareImg: struct (nullable = true)
 |    |-- V21ShareImg: string (nullable = true)
 |-- V21RelImg: struct (nullable = true)
 |    |-- V21RelImg: array (nullable = true)
 |    |    |-- element: string (containsNull = true)
 |-- V21SocImage: struct (nullable = true)
 |    |-- V21SocImage: array (nullable = true)
 |    |    |-- element: string (containsNull = true)
 |-- V21SocVideo: struct (nullable = true)
 |    |-- V21SocVideo: array (nullable = true)
 |    |    |-- element: string (containsNull = true)
 |-- V21Quotations: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- Offset: integer (nullable = true)
 |    |    |-- CharLength: integer (nullable = true)
 |    |    |-- Verb: string (nullable = true)
 |    |    |-- Quote: string (nullable = true)
 |-- V21AllNames: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- Name: string (nullable = true)
 |    |    |-- CharOffset: integer (nullable = true)
 |-- V21Amounts: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- Amount: double (nullable = true)
 |    |    |-- Object: string (nullable = true)
 |    |    |-- Offset: integer (nullable = true)
 |-- V21TransInfo: struct (nullable = true)
 |    |-- Srclc: string (nullable = true)
 |    |-- Eng: string (nullable = true)
 |-- V2ExtrasXML: struct (nullable = true)
 |    |-- Title: string (nullable = true)
 |    |-- Author: string (nullable = true)
 |    |-- Links: string (nullable = true)
 |    |-- AltUrl: string (nullable = true)
 |    |-- AltUrlAmp: string (nullable = true)
 |    |-- PubTimestamp: timestamp (nullable = true)
 ```

## GDELT GKG Columns
```
['GkgRecordId', 'V21Date', 'V2SrcCollectionId', 'V2SrcCmnName', 'V2DocId', 'V1Counts', 'V21Counts', 'V1Themes', 'V2EnhancedThemes', 'V1Locations', 'V2Locations', 'V1Persons', 'V2Persons', 'V1Orgs', 'V2Orgs', 'V15Tone', 'V21EnhancedDates', 'V2GCAM', 'V21ShareImg', 'V21RelImg', 'V21SocImage', 'V21SocVideo', 'V21Quotations', 'V21AllNames', 'V21Amounts', 'V21TransInfo', 'V2ExtrasXML']
```

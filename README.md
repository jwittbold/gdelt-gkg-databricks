# GDELT Global Knowlegde Graph Pipeline
***Global Database of Events, Language, and Tone***


## Running on Databricks cluster and Azure ADLS Gen 2 Storage via Databricks Connect

* [Understanding GDELT GKG]('#understanding')
* [GKG Data Model]('#gkg')
* [Pipeline Deployment Architecture Model]('#deployment')
* [Ingesting GKG Files]('#ingest')
* [GKG Schema]('#schema')
* [Transforming GKG Files]('#transform')
* [Setting Up Databricks Cluster]('#cluster')
* [Setting Up Databricks Connect]('#connect')
* [Setting Up Azure ADLS Gen2 Storage]('#adls')
* [Create Service Principal]('#service')
* [Set Permissions]('#permissions')
* [Environment_Variables]('#environment')
* [Deploy to Cluster]('#deploy')
* [Pipeline Exection]('#execution')
* [Monitoring Pipeline]('monitoring')


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


## One randonly selected row in a raw GKG file broken down to its 27 native columns. 
Highlighting the denormalized structure of the data. Before transformation each CSV file contains 27 columns, many of which contain further nested data elements which are delimited differently depending on the column.

<details>
<summary>

```
 Expand to view raw. Scroll right to view further...
```

</summary>

 ```
20211021000000-0  

20211021000000  

1	 

bryantimes.com	 

https://www.bryantimes.com/news/local/brown-supporting-child-suicide-prevention-bill/article_cd0acb20-a05e-5cf3-b9dc-3949a8746dfb.html  

KILL#13##1#United States#US#US#39.828175#-98.5795#US;CRISISLEX_T03_DEAD#13##1#United States#US#US#39.828175#-98.5795#US;CRISISLEX_T03_DEAD#13##1#United   States#US#US#39.828175#-98.5795#US;  

KILL#13##1#United States#US#US#39.828175#-98.5795#US#1402;CRISISLEX_T03_DEAD#13##1#United States#US#US#39.828175#-98.5795#US#1402;CRISISLEX_T03_DEAD#13##1#United States#US#US#39.828175#-98.5795#US#1402;  

MEDIA_MSM;TAX_FNCACT;TAX_FNCACT_CHILD;SOC_SUICIDE;WB_2024_ANTI_CORRUPTION_AUTHORITIES;WB_696_PUBLIC_SECTOR_MANAGEMENT;WB_831_GOVERNANCE;WB_832_ANTI_CORRUPTION;WB_2026_PREVENTION;CRISISLEX_CRISISLEXREC;TAX_ETHNICITY;TAX_ETHNICITY_AMERICANS;GENERAL_HEALTH;HEALTH_PANDEMIC;WB_635_PUBLIC_HEALTH;WB_2165_HEALTH_EMERGENCIES;WB_2166_HEALTH_EMERGENCY_PREPAREDNESS_AND_DISASTER_RESPONSE;WB_621_HEALTH_NUTRITION_AND_POPULATION;WB_2167_PANDEMICS;UNGP_HEALTHCARE;DEMOCRACY;LEADER;TAX_FNCACT_CONGRESSWOMAN;USPEC_POLITICS_GENERAL1;MEDICAL;TAX_FNCACT_DOCTORS;CRISISLEX_C03_WELLBEING_HEALTH;WB_1406_DISEASES;WB_1430_MENTAL_HEALTH;WB_1427_NON_COMMUNICABLE_DISEASE_AND_INJURY;UNGP_FORESTS_RIVERS_OCEANS;UNGP_CRIME_VIOLENCE;EPU_CATS_HEALTHCARE;TAX_ETHNICITY_AMERICAN;SOC_POINTSOFINTEREST;SOC_POINTSOFINTEREST_HOSPITAL;EDUCATION;SOC_POINTSOFINTEREST_COLLEGES;TAX_FNCACT_CHILDREN;TAX_ETHNICITY_BLACK;KILL;CRISISLEX_T03_DEAD;TAX_FNCACT_PEERS;TAX_FNCACT_PRINCIPAL;TAX_FNCACT_INVESTIGATOR;AFFECT;WB_2670_JOBS;WB_1467_EDUCATION_FOR_ALL;WB_470_EDUCATION;WB_2131_EMPLOYABILITY_SKILLS_AND_JOBS;WB_1484_EDUCATION_SKILLS_DEVELOPMENT_AND_LABOR_MARKET;SOC_POINTSOFINTEREST_SCHOOLS;  

MEDIA_MSM,40;EDUCATION,1036;SOC_POINTSOFINTEREST_COLLEGES,1036;TAX_FNCACT_CHILD,109;TAX_FNCACT_CHILD,511;TAX_FNCACT_CHILD,859;TAX_FNCACT_CHILD,2599;KILL,1457;CRISISLEX_T03_DEAD,1457;SOC_POINTSOFINTEREST_HOSPITAL,977;SOC_POINTSOFINTEREST_HOSPITAL,1733;CRISISLEX_CRISISLEXREC,210;CRISISLEX_CRISISLEXREC,1899;TAX_FNCACT_INVESTIGATOR,1612;TAX_ETHNICITY_AMERICANS,224;SOC_POINTSOFINTEREST_SCHOOLS,2297;SOC_SUICIDE,117;SOC_SUICIDE,165;SOC_SUICIDE,402;SOC_SUICIDE,519;SOC_SUICIDE,718;SOC_SUICIDE,1101;SOC_SUICIDE,1380;SOC_SUICIDE,1468;SOC_SUICIDE,1638;SOC_SUICIDE,1792;SOC_SUICIDE,1859;SOC_SUICIDE,2769;GENERAL_HEALTH,308;GENERAL_HEALTH,2038;GENERAL_HEALTH,2169;HEALTH_PANDEMIC,308;HEALTH_PANDEMIC,2038;HEALTH_PANDEMIC,2169;WB_635_PUBLIC_HEALTH,308;WB_635_PUBLIC_HEALTH,2038;WB_635_PUBLIC_HEALTH,2169;WB_2165_HEALTH_EMERGENCIES,308;WB_2165_HEALTH_EMERGENCIES,2038;WB_2165_HEALTH_EMERGENCIES,2169;WB_2166_HEALTH_EMERGENCY_PREPAREDNESS_AND_DISASTER_RESPONSE,308;WB_2166_HEALTH_EMERGENCY_PREPAREDNESS_AND_DISASTER_RESPONSE,2038;WB_2166_HEALTH_EMERGENCY_PREPAREDNESS_AND_DISASTER_RESPONSE,2169;WB_621_HEALTH_NUTRITION_AND_POPULATION,308;WB_621_HEALTH_NUTRITION_AND_POPULATION,2038;WB_621_HEALTH_NUTRITION_AND_POPULATION,2169;WB_2167_PANDEMICS,308;WB_2167_PANDEMICS,2038;WB_2167_PANDEMICS,2169;UNGP_HEALTHCARE,308;UNGP_HEALTHCARE,2038;UNGP_HEALTHCARE,2169;WB_2024_ANTI_CORRUPTION_AUTHORITIES,128;WB_2024_ANTI_CORRUPTION_AUTHORITIES,530;WB_2024_ANTI_CORRUPTION_AUTHORITIES,1112;WB_2024_ANTI_CORRUPTION_AUTHORITIES,1649;WB_696_PUBLIC_SECTOR_MANAGEMENT,128;WB_696_PUBLIC_SECTOR_MANAGEMENT,530;WB_696_PUBLIC_SECTOR_MANAGEMENT,1112;WB_696_PUBLIC_SECTOR_MANAGEMENT,1649;WB_831_GOVERNANCE,128;WB_831_GOVERNANCE,530;WB_831_GOVERNANCE,1112;WB_831_GOVERNANCE,1649;WB_832_ANTI_CORRUPTION,128;WB_832_ANTI_CORRUPTION,530;WB_832_ANTI_CORRUPTION,1112;WB_832_ANTI_CORRUPTION,1649;WB_2026_PREVENTION,128;WB_2026_PREVENTION,530;WB_2026_PREVENTION,1112;WB_2026_PREVENTION,1649;WB_2670_JOBS,2270;WB_1467_EDUCATION_FOR_ALL,2270;WB_470_EDUCATION,2270;WB_2131_EMPLOYABILITY_SKILLS_AND_JOBS,2270;WB_1484_EDUCATION_SKILLS_DEVELOPMENT_AND_LABOR_MARKET,2270;TAX_ETHNICITY_AMERICAN,931;TAX_ETHNICITY_AMERICAN,968;TAX_ETHNICITY_AMERICAN,1019;AFFECT,2182;TAX_FNCACT_PEERS,1491;UNGP_CRIME_VIOLENCE,706;LEADER,469;TAX_FNCACT_CONGRESSWOMAN,469;USPEC_POLITICS_GENERAL1,469;MEDICAL,589;TAX_FNCACT_DOCTORS,589;CRISISLEX_C03_WELLBEING_HEALTH,589;EPU_CATS_HEALTHCARE,788;EPU_CATS_HEALTHCARE,1150;TAX_FNCACT_PRINCIPAL,1599;DEMOCRACY,455;WB_1406_DISEASES,614;WB_1430_MENTAL_HEALTH,614;WB_1427_NON_COMMUNICABLE_DISEASE_AND_INJURY,614;TAX_FNCACT_CHILDREN,1277;TAX_FNCACT_CHILDREN,1724;UNGP_FORESTS_RIVERS_OCEANS,658;TAX_ETHNICITY_BLACK,1393;TAX_ETHNICITY_BLACK,1408;TAX_ETHNICITY_BLACK,1544;TAX_ETHNICITY_BLACK,1801;TAX_ETHNICITY_BLACK,1843;	1#United States#US#US#39.828175#-98.5795#US  

1#Americans#US#US##39.828175#-98.5795#US#224;1#American#US#US##39.828175#-98.5795#US#931;1#American#US#US##39.828175#-98.5795#US#968;1#American#US#US##39.828175#-9
8.5795#US#1019  

arielle sheftall;sherrod brown;brown d-ohio  

Arielle Sheftall,1587;Sherrod Brown,74	 

american academy of pediatrics;american hospital association;association of american medical colleges;academy of child	 

American Academy Of Pediatrics,953;American Hospital Association,989;Association Of American Medical Colleges,1036;Academy Of Child,2599	 

-6.14406779661017,1.69491525423729,7.83898305084746,9.53389830508475,27.5423728813559,2.11864406779661,432	 

1#0#0#1999#1965	 

wc:432,c12.1:44,c12.10:73,c12.12:35,c12.13:13,c12.14:27,c12.3:22,c12.4:10,c12.5:14,c12.7:55,c12.8:22,c12.9:39,c13.12:1,c13.2:1,c14.1:34,c14.10:23,c14.11:75,c14.2:40,c14.3:40,c14.4:7,c14.5:82,c14.6:2,c14.7:10,c14.8:3,c14.9:8,c15.10:3,c15.102:1,c15.103:1,c15.110:1,c15.112:1,c15.116:2,c15.131:1,c15.132:1,c15.137:1,c15.143:1,c15.148:7,c15.149:2,c15.15:5,c15.152:1,c15.154:1,c15.159:1,c15.168:1,c15.171:2,c15.172:1,c15.173:1,c15.176:2,c15.178:2,c15.18:2,c15.197:3,c15.198:2,c15.20:2,c15.201:4,c15.202:1,c15.212:1,c15.217:2,c15.22:2,c15.227:1,c15.231:2,c15.24:1,c15.241:6,c15.248:1,c15.251:2,c15.252:4,c15.255:1,c15.256:2,c15.257:1,c15.26:1,c15.27:1,c15.270:2,c15.3:2,c15.4:1,c15.42:1,c15.48:2,c15.53:2,c15.57:2,c15.7:2,c15.71:1,c15.72:1,c15.83:1,c15.86:1,c15.9:1,c16.1:1,c16.100:3,c16.101:3,c16.103:2,c16.105:2,c16.106:15,c16.109:29,c16.11:3,c16.110:83,c16.111:4,c16.113:1,c16.114:24,c16.115:3,c16.116:12,c16.117:17,c16.118:27,c16.12:38,c16.120:27,c16.121:27,c16.122:3,c16.123:1,c16.124:4,c16.125:44,c16.126:38,c16.127:32,c16.128:16,c16.129:74,c16.130:5,c16.131:15,c16.132:1,c16.134:47,c16.137:1,c16.138:25,c16.139:13,c16.140:22,c16.145:46,c16.146:32,c16.147:5,c16.149:1,c16.15:1,c16.152:7,c16.153:18,c16.155:5,c16.156:1,c16.157:16,c16.158:3,c16.159:39,c16.16:5,c16.161:21,c16.162:9,c16.163:18,c16.164:9,c16.165:2,c16.17:1,c16.19:7,c16.2:20,c16.21:3,c16.22:16,c16.23:3,c16.24:5,c16.26:69,c16.27:6,c16.29:3,c16.3:8,c16.31:48,c16.32:8,c16.33:43,c16.34:7,c16.35:31,c16.36:15,c16.37:48,c16.38:14,c16.39:1,c16.4:31,c16.41:20,c16.42:3,c16.45:14,c16.46:1,c16.47:58,c16.48:5,c16.49:1,c16.50:9,c16.51:4,c16.52:13,c16.53:6,c16.54:3,c16.56:7,c16.57:241,c16.58:38,c16.59:1,c16.6:74,c16.60:4,c16.61:1,c16.62:14,c16.63:12,c16.64:5,c16.65:10,c16.66:8,c16.68:29,c16.69:13,c16.7:23,c16.70:12,c16.71:2,c16.72:4,c16.73:4,c16.74:5,c16.75:17,c16.76:4,c16.77:2,c16.78:7,c16.79:2,c16.80:1,c16.81:7,c16.83:1,c16.84:15,c16.85:3,c16.87:73,c16.88:66,c16.89:13,c16.90:12,c16.91:13,c16.92:60,c16.93:2,c16.94:24,c16.95:24,c16.96:11,c16.97:4,c16.98:39,c16.99:1,c17.1:134,c17.10:55,c17.11:48,c17.12:20,c17.13:6,c17.14:6,c17.15:31,c17.16:5,c17.17:1,c17.18:5,c17.19:43,c17.2:9,c17.20:5,c17.21:3,c17.22:11,c17.23:8,c17.24:36,c17.25:3,c17.26:1,c17.27:65,c17.28:9,c17.29:19,c17.3:1,c17.30:20,c17.31:45,c17.32:29,c17.33:34,c17.34:10,c17.35:9,c17.36:22,c17.37:24,c17.38:6,c17.39:13,c17.4:105,c17.40:16,c17.41:22,c17.42:36,c17.43:30,c17.5:111,c17.6:7,c17.7:54,c17.8:77,c17.9:5,c18.1:1,c18.100:1,c18.13:1,c18.139:1,c18.147:3,c18.149:9,c18.180:10,c18.190:3,c18.193:11,c18.270:13,c18.298:4,c18.34:13,c18.342:7,c18.78:1,c2.1:25,c2.10:2,c2.100:2,c2.101:9,c2.102:11,c2.104:86,c2.107:2,c2.108:1,c2.109:1,c2.11:11,c2.110:4,c2.111:1,c2.112:13,c2.113:7,c2.114:29,c2.115:1,c2.116:17,c2.117:1,c2.118:17,c2.119:134,c2.12:21,c2.120:1,c2.121:27,c2.122:13,c2.123:2,c2.124:7,c2.125:22,c2.126:20,c2.127:48,c2.128:9,c2.129:19,c2.130:2,c2.131:2,c2.132:3,c2.133:1,c2.134:3,c2.135:1,c2.136:2,c2.137:1,c2.138:1,c2.139:3,c2.14:48,c2.140:1,c2.141:7,c2.142:10,c2.143:31,c2.144:10,c2.145:5,c2.146:5,c2.147:66,c2.148:41,c2.149:1,c2.15:26,c2.150:6,c2.151:1,c2.152:2,c2.153:22,c2.154:7,c2.155:48,c2.156:23,c2.157:42,c2.158:39,c2.159:2,c2.160:18,c2.162:9,c2.163:2,c2.166:11,c2.169:12,c2.17:5,c2.170:12,c2.171:8,c2.173:7,c2.175:1,c2.176:3,c2.177:36,c2.179:32,c2.18:13,c2.180:14,c2.181:16,c2.183:16,c2.185:115,c2.186:8,c2.187:35,c2.19:2,c2.191:7,c2.192:18,c2.193:22,c2.195:49,c2.196:18,c2.197:14,c2.198:29,c2.199:8,c2.2:2,c2.20:1,c2.200:7,c2.201:2,c2.203:26,c2.204:36,c2.205:7,c2.206:4,c2.207:7,c2.209:11,c2.210:49,c2.211:1,c2.213:28,c2.214:20,c2.216:6,c2.217:30,c2.218:3,c2.219:9,c2.220:46,c2.221:4,c2.223:5,c2.225:19,c2.226:12,c2.227:4,c2.228:2,c2.23:9,c2.24:13,c2.25:22,c2.26:28,c2.27:28,c2.28:11,c2.30:14,c2.31:18,c2.32:1,c2.33:8,c2.34:33,c2.35:15,c2.36:12,c2.37:15,c2.39:61,c2.4:2,c2.42:2,c2.43:1,c2.44:17,c2.45:11,c2.46:58,c2.47:2,c2.48:15,c2.49:1,c2.50:7,c2.52:39,c2.53:2,c2.54:43,c2.55:1,c2.56:1,c2.57:7,c2.58:8,c2.59:1,c2.6:2,c2.61:5,c2.62:15,c2.64:9,c2.65:2,c2.66:3,c2.68:2,c2.69:1,c2.70:3,c2.71:2,c2.73:7,c2.74:2,c2.75:83,c2.76:317,c2.77:46,c2.78:62,c2.79:8,c2.80:62,c2.81:5,c2.82:21,c2.83:9,c2.84:9,c2.85:1,c2.86:16,c2.87:8,c2.88:20,c2.89:27,c2.9:1,c2.90:11,c2.93:16,c2.94:2,c2.95:58,c2.96:2,c2.97:2,c2.98:35,c2.99:3,c25.1:4,c25.11:1,c25.5:2,c3.1:38,c3.2:29,c35.1:2,c35.11:4,c35.14:4,c35.15:8,c35.2:2,c35.20:11,c35.25:1,c35.28:1,c35.3:1,c35.31:17,c35.32:12,c35.33:13,c35.4:2,c35.5:5,c35.7:1,c39.1:1,c39.13:1,c39.14:1,c39.17:4,c39.18:1,c39.2:8,c39.20:1,c39.24:1,c39.25:10,c39.26:1,c39.27:1,c39.28:1,c39.3:23,c39.36:2,c39.37:21,c39.39:3,c39.4:22,c39.40:1,c39.41:5,c39.5:12,c39.6:1,c4.1:1,c4.13:4,c4.23:13,c4.3:9,c40.5:3,c41.1:32,c42.1:165,c5.1:1,c5.10:26,c5.11:14,c5.12:67,c5.15:11,c5.16:1,c5.17:12,c5.18:3,c5.19:5,c5.2:19,c5.20:16,c5.21:24,c5.22:3,c5.23:10,c5.24:9,c5.25:6,c5.26:4,c5.27:2,c5.28:12,c5.29:11,c5.30:55,c5.31:3,c5.32:2,c5.33:4,c5.34:15,c5.35:10,c5.36:25,c5.37:11,c5.4:4,c5.40:47,c5.42:2,c5.43:11,c5.44:3,c5.45:11,c5.46:67,c5.47:8,c5.48:4,c5.49:62,c5.50:66,c5.51:41,c5.52:80,c5.53:32,c5.54:20,c5.55:8,c5.56:4,c5.57:2,c5.58:7,c5.6:14,c5.60:21,c5.61:41,c5.62:193,c5.7:9,c5.8:21,c5.9:26,c6.1:3,c6.2:2,c6.3:1,c6.4:22,c6.5:4,c6.6:3,c7.1:50,c7.2:27,c8.1:3,c8.10:5,c8.11:1,c8.13:3,c8.2:3,c8.20:1,c8.23:8,c8.27:2,c8.28:7,c8.3:4,c8.35:1,c8.36:11,c8.37:21,c8.38:8,c8.39:1,c8.4:14,c8.40:2,c8.41:4,c8.42:13,c8.43:20,c8.5:1,c8.6:1,c9.1:18,c9.10:3,c9.1000:1,c9.1011:5,c9.1012:2,c9.1018:7,c9.1030:2,c9.1036:2,c9.1038:2,c9.1039:1,c9.104:1,c9.1040:1,c9.107:5,c9.109:6,c9.11:2,c9.110:2,c9.111:7,c9.113:2,c9.116:1,c9.118:5,c9.119:2,c9.12:1,c9.122:5,c9.123:1,c9.124:2,c9.125:4,c9.127:2,c9.128:27,c9.129:10,c9.130:8,c9.133:11,c9.134:5,c9.135:14,c9.137:3,c9.138:1,c9.141:2,c9.142:1,c9.143:5,c9.145:1,c9.148:1,c9.149:3,c9.15:3,c9.151:6,c9.157:2,c9.158:11,c9.159:2,c9.160:6,c9.161:1,c9.162:10,c9.163:3,c9.164:2,c9.165:1,c9.167:10,c9.168:7,c9.169:2,c9.174:4,c9.175:3,c9.177:6,c9.178:1,c9.179:1,c9.18:2,c9.180:2,c9.182:7,c9.184:9,c9.188:5,c9.195:1,c9.196:2,c9.197:4,c9.2:4,c9.20:2,c9.200:3,c9.201:1,c9.203:3,c9.205:1,c9.207:1,c9.208:1,c9.212:2,c9.215:2,c9.220:1,c9.224:2,c9.227:1,c9.229:1,c9.23:2,c9.231:2,c9.232:1,c9.233:3,c9.235:1,c9.238:3,c9.24:1,c9.245:1,c9.25:2,c9.250:2,c9.253:2,c9.260:2,c9.263:1,c9.265:1,c9.267:1,c9.27:2,c9.270:1,c9.274:1,c9.275:1,c9.281:1,c9.284:1,c9.288:2,c9.289:1,c9.290:4,c9.291:2,c9.292:1,c9.3:15,c9.30:2,c9.302:3,c9.306:1,c9.307:1,c9.310:2,c9.313:1,c9.315:1,c9.317:3,c9.32:3,c9.324:1,c9.326:1,c9.329:1,c9.33:8,c9.330:3,c9.335:3,c9.34:6,c9.35:4,c9.353:1,c9.358:1,c9.359:1,c9.360:1,c9.37:5,c9.370:1,c9.371:6,c9.372:15,c9.374:1,c9.378:1,c9.383:1,c9.384:10,c9.385:8,c9.387:1,c9.389:1,c9.39:8,c9.391:1,c9.393:1,c9.395:1,c9.402:2,c9.405:2,c9.419:2,c9.420:1,c9.423:1,c9.427:1,c9.429:1,c9.43:1,c9.430:2,c9.432:2,c9.433:1,c9.438:5,c9.439:2,c9.44:4,c9.440:2,c9.446:3,c9.447:3,c9.448:1,c9.449:7,c9.45:1,c9.450:7,c9.451:7,c9.452:3,c9.454:2,c9.455:2,c9.456:2,c9.457:2,c9.458:3,c9.459:8,c9.46:4,c9.463:2,c9.466:4,c9.467:2,c9.468:2,c9.47:3,c9.470:1,c9.474:5,c9.476:7,c9.477:3,c9.478:4,c9.479:9,c9.48:4,c9.480:13,c9.481:3,c9.483:2,c9.485:4,c9.487:1,c9.488:1,c9.489:3,c9.49:1,c9.491:3,c9.492:2,c9.494:4,c9.496:9,c9.497:1,c9.498:8,c9.499:3,c9.5:1,c9.501:5,c9.502:1,c9.503:1,c9.504:5,c9.507:5,c9.509:6,c9.511:8,c9.513:12,c9.517:1,c9.519:6,c9.521:1,c9.522:8,c9.523:4,c9.524:4,c9.53:1,c9.537:1,c9.538:2,c9.540:5,c9.542:2,c9.545:2,c9.546:2,c9.549:1,c9.55:7,c9.551:4,c9.556:5,c9.557:6,c9.559:2,c9.560:5,c9.561:1,c9.562:4,c9.564:3,c9.567:3,c9.568:1,c9.569:2,c9.57:3,c9.570:5,c9.571:2,c9.574:2,c9.575:5,c9.576:1,c9.579:16,c9.581:9,c9.583:1,c9.585:2,c9.588:5,c9.589:3,c9.59:2,c9.590:3,c9.591:2,c9.600:10,c9.604:1,c9.608:1,c9.61:1,c9.613:1,c9.616:3,c9.617:1,c9.618:4,c9.619:1,c9.62:1,c9.620:1,c9.622:2,c9.624:5,c9.625:4,c9.626:1,c9.627:3,c9.629:2,c9.632:2,c9.635:2,c9.638:1,c9.64:2,c9.640:7,c9.642:13,c9.645:2,c9.647:3,c9.648:13,c9.649:6,c9.653:23,c9.654:7,c9.655:2,c9.658:3,c9.659:3,c9.66:6,c9.660:10,c9.661:2,c9.663:2,c9.664:1,c9.665:4,c9.666:1,c9.667:3,c9.668:2,c9.669:4,c9.67:1,c9.670:10,c9.671:8,c9.672:1,c9.673:5,c9.674:2,c9.676:3,c9.677:4,c9.678:1,c9.679:3,c9.680:2,c9.682:3,c9.683:10,c9.684:3,c9.686:5,c9.687:7,c9.690:11,c9.691:1,c9.692:7,c9.693:7,c9.694:1,c9.696:2,c9.697:2,c9.698:4,c9.7:1,c9.70:8,c9.701:13,c9.704:8,c9.705:1,c9.708:5,c9.71:3,c9.710:3,c9.712:2,c9.713:1,c9.719:1,c9.72:1,c9.720:2,c9.722:1,c9.723:3,c9.724:3,c9.725:2,c9.726:23,c9.730:16,c9.732:1,c9.734:4,c9.735:7,c9.736:3,c9.737:4,c9.739:1,c9.74:1,c9.740:6,c9.741:3,c9.745:3,c9.746:2,c9.748:10,c9.752:2,c9.755:3,c9.757:4,c9.759:1,c9.76:8,c9.760:1,c9.762:13,c9.763:2,c9.765:1,c9.767:27,c9.768:2,c9.770:2,c9.771:4,c9.772:1,c9.774:3,c9.776:2,c9.778:1,c9.78:1,c9.781:3,c9.788:1,c9.789:2,c9.790:3,c9.792:2,c9.793:1,c9.795:1,c9.8:2,c9.80:1,c9.802:5,c9.806:4,c9.807:2,c9.808:7,c9.812:4,c9.816:4,c9.818:1,c9.82:5,c9.821:3,c9.823:1,c9.824:1,c9.826:1,c9.83:8,c9.831:1,c9.833:3,c9.834:4,c9.837:3,c9.838:1,c9.840:3,c9.845:4,c9.846:4,c9.853:3,c9.855:1,c9.857:1,c9.858:2,c9.860:10,c9.861:2,c9.862:1,c9.863:4,c9.864:13,c9.865:4,c9.867:3,c9.868:13,c9.870:2,c9.872:1,c9.873:2,c9.874:1,c9.877:2,c9.878:2,c9.879:1,c9.88:1,c9.882:11,c9.883:7,c9.884:3,c9.889:1,c9.89:3,c9.890:1,c9.893:1,c9.896:4,c9.897:4,c9.898:4,c9.899:1,c9.90:4,c9.900:1,c9.901:3,c9.902:4,c9.903:3,c9.904:3,c9.908:9,c9.911:5,c9.915:2,c9.916:1,c9.920:1,c9.923:4,c9.926:8,c9.930:8,c9.935:11,c9.938:2,c9.940:1,c9.942:1,c9.945:3,c9.946:6,c9.948:1,c9.95:2,c9.953:1,c9.955:3,c9.96:5,c9.963:3,c9.964:2,c9.965:1,c9.966:6,c9.969:1,c9.972:8,c9.973:1,c9.975:1,c9.976:5,c9.977:5,c9.978:8,c9.98:2,c9.980:1,c9.981:1,c9.984:4,c9.985:3,c9.986:8,c9.987:1,c9.992:1,v10.1:0.2864453125,v10.2:0.265868019869818,v11.1:-0.00909062318840579,v19.1:5.11492957746479,v19.2:5.39,v19.3:4.89239436619718,v19.4:5.13014084507042,v19.5:5.20225352112676,v19.6:5.20718309859155,v19.7:5.11971830985916,v19.8:5.56718309859155,v19.9:4.6343661971831,v20.1:0.325,v20.10:-0.589230769230769,v20.11:0.492857142857143,v20.12:-0.5725,v20.13:0.392060606060606,v20.14:-0.546052631578947,v20.15:0.336814814814815,v20.16:-0.53125,v20.2:-0.357,v20.3:0.325,v20.4:-0.339166666666667,v20.5:0.325,v20.6:-0.545555555555556,v20.7:0.325,v20.8:-0.582727272727273,v20.9:0.475,v21.1:5.32725888324873,v26.1:-0.816,v42.10:-0.0928580947575758,v42.11:-0.105255434369697,v42.2:0.143426244818182,v42.3:0.0989954048121212,v42.4:0.0975243302060606,v42.5:0.102464749145455,v42.6:0.0962527494424242,v42.7:-0.125047707242424,v42.8:-0.0939613242060606,v42.9:-0.0576108369636364	 

https://bloximages.chicago2.vip.townnews.com/bryantimes.com/content/tncms/custom/image/45f9f6d2-e0e6-11e7-954d-4f5f4b273a47.jpg			

https://youtube.com/user/bryantimes;		

Sherrod Brown,78;Child Suicide Prevention,132;Lethal Means Safety,156;Democratic Congresswoman Lauren Underwood,514;Child Suicide Prevention,563;Lethal Means   Safety,587;American Academy,992;American Hospital Association,1045;American Medical,1085;Arielle Sheftall,1661;Suicide Prevention,1726;Abigail Wexner Research   Institute,1780;Nationwide Children,1803;Adolescent Psychology,2770	 

4,leading cause of death,1602;		

<PAGE_AUTHORS>Lucas Bechtol lbechtol@bryantimes.com;ltbechtol</PAGE_AUTHORS><PAGE_PRECISEPUBTIMESTAMP>20211020231500</PAGE_PRECISEPUBTIMESTAMP><PAGE_TITLE>Brown supporting child suicide prevention bill</PAGE_TITLE>
 
```
</details>

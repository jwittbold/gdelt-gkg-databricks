import datetime
from decimal import Decimal
import datetime
import sys
import os
from pyspark.sql.types import Row




class GkgRecord:

    def __init__(self):
        self.date = int(0)
        self.translingual = bool()
        self.number_in_batch = int(0)

    def values(self):
        return self.date,self.translingual,self.number_in_batch

class V21Date:

    def __init__(self, parsed_date):
        self.parsed_date = datetime.datetime.strptime(parsed_date, '%Y%m%d%H%M%S')
    
    def values(self):
        return self.parsed_date


class V2SrcCollectionId:

    def __init__(self):
        self.id = str('')

    def values(self):
        return self.id


class V1Count:
    def __init__(self):
        self.count_type = str('')
        self.count = int(0)
        self.object_type = str('')
        self.location_type = int(0)
        self.location_name = str('')
        self.country_code = str('')
        self.ADM1_code = str('')
        self.latitude = Decimal(0.0)
        self.longitude = Decimal(0.0)
        self.feature_id = str('')

    def values(self):
        return self.count_type,self.count,self.object_type,self.location_type,self.location_name, \
            self.country_code,self.ADM1_code,self.latitude,self.longitude,self.feature_id


class V21Count:
    def __init__(self):
        self.count_type = str('')
        self.count = int(0)
        self.object_type = str('')
        self.location_type = int(0)
        self.location_name = str('')
        self.country_code = str('')
        self.ADM1_code = str('')
        self.latitude = Decimal(0.0)
        self.longitude = Decimal(0.0)
        self.feature_id = str('')
        self.char_offset = int(0)


    def values(self):
        return self.count_type,self.count,self.object_type,self.location_type,self.location_name, \
            self.country_code,self.ADM1_code,self.latitude,self.longitude,self.feature_id,self.char_offset


class Location:

    def __init__(self):
            self.location_type = int(0)
            self.location_full_name = str('')
            self.location_country_code = str('')
            self.location_ADM1_code = str('')
            self.location_latitude = Decimal(0.0)
            self.location_longitude = Decimal(0.0)
            self.location_feature_id = str('')

    def values(self):
        return self.location_type,self.location_full_name,self.location_country_code,self.location_ADM1_code, \
            self.location_latitude,self.location_longitude,self.location_feature_id


class EnhancedLocation:

    def __init__(self):
            self.type = int(0)
            self.location_name = str('')
            self.country_code = str('')
            self.ADM1_code = str('')
            self.ADM2_code = str('')
            self.latitude = Decimal(0.0)
            self.longitude = Decimal(0.0)
            self.feature_id = str('')
            self.char_offset = int(0)

    def values(self):
        return self.type,self.location_name,self.country_code,self.ADM1_code,self.ADM2_code, \
            self.latitude,self.longitude,self.feature_id,self.char_offset


class EnhancedPerson:

    def __init__(self):
        self.person = str('')
        self.char_offset = int(0)

    def values(self):
        return self.person,self.char_offset


class EnhancedTheme:

    def __init__(self):
        self.theme = str('')
        self.char_offset = int(0)
    
    def values(self):
        return self.theme,self.char_offset


class EnhancedDate:

    def __init__(self):
        self.date_resolution = int(0)
        self.month = int(0)
        self.day = int(0)
        self.year = int(0)
        self.char_offset = int(0)
    
    def values(self):
        return self.date_resolution,self.month,self.day,self.year,self.char_offset



class EnhancedOrg:

    def __init__(self):
        self.organization = str('')
        self.char_offset = int(0)

    def values(self):
        return self.organization,self.char_offset


class Amount:

    def __init__(self):
        self.amount = Decimal(0)
        self.object = str('')
        self.char_offset = int(0)
    
    def values(self):
        return self.amount,self.object,self.char_offset


class Tone:

    def __init__(self):
        self.tone = float(0.0)
        self.positive_score = float(0.0)
        self.negative_score = float(0.0)
        self.polarity = float(0.0)
        self.activity_reference_density = float(0.0)
        self.self_group_reference_density = float(0.0)
        self.word_count = int(0)

    def values(self):
        return self.tone,self.positive_score,self.negative_score,self.polarity,self.activity_reference_density, \
            self.self_group_reference_density,self.word_count


class Name:

    def __init__(self):
        self.name = str('')
        self.char_offset = int(0)

    def values(self):
        return self.name,self.char_offset


class Quotation:

    def __init__(self):

        self.char_offset = int(0)
        self.char_length = int(0)
        self.verb = str('')
        self.quote = str('')

    def values(self):
        return self.char_offset,self.char_length,self.verb,self.quote


class TranslationInfo:

    def __init__(self):
        self.srclc = str('')
        self.eng = str('')
    
    def values(self):

        if self.srclc == '':
            self.srclc == None
        if self.eng == '':
            self.eng = None

        return self.srclc,self.eng


class Gcam:

    def __init__(self):

        self.dictionary_dimension_id = str('')
        self.score = float(0.0)

    def values(self):
        return self.dictionary_dimension_id,self.score


class V2ExtrasXML:

    def __init__(self):

        self.title = str('')
        self.authors = str('')
        self.links = str('')
        self.alt_url = str('')
        self.alt_url_mobile = str('')
        self.pub_timestamp = str('')
        # self.book_title = str('')
        # self.date = str('')
        # self.journal = str('')
        # self.volume = str('')
        # self.pages = str('')
        # self.publisher = str('')
        # self.location = str('')
        # self.marker = str('')

    def values(self):
        
        if self.title == '':
            self.title = None
        if self.authors == '':
            self.authors = None
        if self.links == '':
            self.links = None
        if self.alt_url == '':
            self.alt_url = None
        if self.alt_url_mobile == '':
            self.alt_url_mobile = None
        if self.pub_timestamp == '':
            self.alt_url_mobile = None

        return self.title,self.authors,self.links,self.alt_url,self.alt_url_mobile,self.pub_timestamp


def gkg_parser(line: str):

    val = line.split('\t', -1)

    if len(val) == 27:

        gkg_record_id = create_gkg_record_id(val[0])                        # 0
        v21_date = create_v21_date(val[1])                                  # 1
        v2_src_collection_id = create_v2_src_collection_id(val[2])          # 2
        v2_src_common_name = create_v2_src_common_name(val[3])              # 3
        v2_doc_id = create_v2_doc_id(val[4])                                # 4
        v1_counts = create_v1_count_array(val[5])                           # 5
        v21_counts = create_v21_count_array(val[6])                         # 6
        v1_themes = create_v1_themes_array(val[7])                          # 7
        v2_enhanced_themes = create_v2_enhanced_themes_array(val[8])        # 8
        v1_locations = create_v1_locations_array(val[9])                    # 9
        v2_enhanced_locations = create_v2_enhanced_locations_array(val[10]) # 10
        v1_persons = create_v1_persons_array(val[11])                       # 11
        v2_enhanced_persons = create_v2_enhanced_persons(val[12])           # 12
        v1_orgs = create_v1_orgs(val[13])                                   # 13
        v2_enhanced_orgs = create_v2_enhanced_orgs(val[14])                 # 14
        v15_tone = create_v15_tone(val[15])                                 # 15
        v21_enhanced_dates = create_v21_enhanced_dates(val[16])             # 16
        v2_gcam = create_v2_gcam(val[17])                                   # 17
        v21_share_img = create_v21_share_img(val[18])                       # 18
        v21_rel_img = create_v21_rel_img(val[19])                           # 19
        v21_soc_img = create_v21_soc_img(val[20])                           # 20
        v21_soc_vid = create_v21_soc_vid(val[21])                           # 21
        v21_quotes = create_v21_quotes_array(val[22])                       # 22
        v21_all_names = create_v21_all_names(val[23])                       # 23
        v21_amounts = create_v21_amounts(val[24])                           # 24
        v21_trans_info = create_v21_trans_info(val[25])                     # 25
        v2_extras_xml = create_v2_extras_xml(val[26])                       # 26


        gkg_record = gkg_record_id, v21_date, v2_src_collection_id, v2_src_common_name, \
                    v2_doc_id, v1_counts, v21_counts, v1_themes, v2_enhanced_themes, \
                    v1_locations, v2_enhanced_locations, v1_persons, v2_enhanced_persons, \
                    v1_orgs, v2_enhanced_orgs, v15_tone, v21_enhanced_dates, v2_gcam, \
                    v21_share_img, v21_rel_img, v21_soc_img, v21_soc_vid, v21_quotes, \
                    v21_all_names, v21_amounts, v21_trans_info, v2_extras_xml
        return gkg_record

    else:
        corrupt_record = None, None, None, None, None, None, None, None, None, None, \
                    None, None, None, None, None, None, None, None, None, None, \
                    None, None, None, None, None, None, None
        return corrupt_record
        


####################################################################################
#######################             GgkRecordID         ############################
####################################################################################

# 0
def create_gkg_record_id(str_: str):

    gkg_record_id = GkgRecord()

    if str_ != '':
        val = str_.split('-')

        gkg_record_id.date = int(val[0])

        if val[1].startswith('T'):
            gkg_record_id.translingual = True
            gkg_record_id.number_in_batch = int(val[1].split('T')[1])
        else:
            gkg_record_id.translingual = False
            gkg_record_id.number_in_batch = int(val[1])

    return gkg_record_id.values()




####################################################################################
#######################              V21Date            ############################
####################################################################################

# 1
def create_v21_date(str_: str):

    v21_date = V21Date(parsed_date=str_)

    return Row(v21_date.values())




####################################################################################
####################          V2SrcCollectionId         ############################
####################################################################################

# 2
def create_v2_src_collection_id(str_: str):

    v2_src_collection_id = V2SrcCollectionId()
    
    switch = {
        '1': 'WEB',
        '2': 'CITATIONONLY',
        '3': 'CORE',
        '4': 'DTIC',
        '5': 'JSTOR',
        '6': 'NONTEXTUALSOURCE',
        '_': 'WEB'
        }

    switched = switch.get(str_, 'null')

    v2_src_collection_id.id = switched

    return Row(v2_src_collection_id.values())



####################################################################################
####################            V2SrcCmnName            ############################
####################################################################################

# 3 
def create_v2_src_common_name(str_: str) -> Row:

    src_common_name = str_

    return Row(src_common_name)



####################################################################################
####################               V2DocId              ############################
####################################################################################

# 4
def create_v2_doc_id(str_: str) -> Row:

    v2_doc_id = str_

    return Row(v2_doc_id)



####################################################################################
####################               V1Counts             ############################
####################################################################################

# 5
def create_v1_count(str_: str):

    v1_count = V1Count()

    field = str_.split('#')
    if len(field) > 9:
        
        v1_count.count_type = str(field[0])
        v1_count.count = int(field[1])
        v1_count.object_type = str(field[2])
        v1_count.location_type = int(field[3])
        v1_count.location_name = str(field[4])
        v1_count.country_code = str(field[5])
        v1_count.ADM1_code = str(field[6])
        if field[7] != '':    
            v1_count.latitude = Decimal(field[7])
        if field[8] != '':
            v1_count.longitude = Decimal(field[8])
        v1_count.feature_id = str(field[9])

    return v1_count.values()


def create_v1_count_array(str_: str):

    array = []
    if str_ != '':
        val = str_.split(';')
        for item in val:
            v1_count = create_v1_count(item)
            array.append(v1_count)
            
    if array: return array[:-1]



####################################################################################
####################              V21Counts             ############################
####################################################################################

# 6
def create_v21_count(str_: str):

    v21_count = V21Count()

    blocks = str_.split('#')
    if len(blocks) > 9:

        v21_count.count_type = str(blocks[0])
        v21_count.count = int(blocks[1])
        v21_count.object_type = str(blocks[2])
        v21_count.location_type = int(blocks[3])
        v21_count.location_name = str(blocks[4])
        v21_count.country_code = str(blocks[5])
        v21_count.ADM1_code = str(blocks[6])
        if blocks[7] != '':    
            v21_count.latitude = Decimal(blocks[7])
        if blocks[8] != '':
            v21_count.longitude = Decimal(blocks[8])
        v21_count.feature_id = str(blocks[9])
        v21_count.char_offset = int(blocks[10])

    return v21_count.values()


def create_v21_count_array(str_: str):

    array = []
    if str_ != '':
        val = str_.split(';')
        for item in val:
            v21_count = create_v21_count(item)
            array.append(v21_count)
            
    if array: return array[:-1]



####################################################################################
#######################             V1Theme             ############################
####################################################################################

# 7
def create_v1_themes_array(str_: str):

    array = []
    if str_ != '':
        val = str_.split(';')
        for item in val:
            array.append(item)
    
    if array: return Row(array[:-1])



####################################################################################
#######################             V2Theme             ############################
####################################################################################

# 8
def create_v2_enhanced_theme(str_: str):

    enhanced_theme = EnhancedTheme()
    if str_ != '':
        field = str_.split(',')
        if len(field) == 2:

            enhanced_theme.theme = str(field[0])
            enhanced_theme.char_offset = int(field[1])

    return enhanced_theme.values()


def create_v2_enhanced_themes_array(str_: str):

    array = []
    if str_ != '':
        val = str_.split(';')
        for item in val:
            v2_enhanced_theme = create_v2_enhanced_theme(item)
            array.append(v2_enhanced_theme)

    if array: return array[:-1]



####################################################################################
######################             V1Location             ##########################
####################################################################################

# 9
def create_v1_location(str_: str):

    loc = Location()
    
    blocks = str_.split('#')
    if len(blocks) == 7:
        loc.location_type = int(blocks[0])
        loc.location_full_name = str(blocks[1])
        loc.location_country_code = str(blocks[2])
        loc.location_ADM1_code = str(blocks[3])
        if blocks[4] != '':
            loc.location_latitude = Decimal(blocks[4])
        if blocks[5] != '':
            loc.location_longitude = Decimal(blocks[5])
        loc.location_feature_id = str(blocks[6])

    return loc.values()


def create_v1_locations_array(str_: str):

    array = []
    if str_ != '':
        val = str_.split(';')
        for item in val:
            v1_location = create_v1_location(item)
            array.append(v1_location)

    if array: return array




####################################################################################
##################            V2 ENHANCED LOCATIONS           ######################
####################################################################################

# 10
def create_v2_enhanced_location(str_: str):

    v2_enhanced_location = EnhancedLocation()

    block = str_.split('#')
    if len(block) == 9:

        v2_enhanced_location.type = int(block[0])
        v2_enhanced_location.location_name = str(block[1])
        v2_enhanced_location.country_code = str(block[2])
        v2_enhanced_location.ADM1_code = str(block[3])
        v2_enhanced_location.ADM2_code = str(block[4])
        if block[5] != '':
            v2_enhanced_location.latitude = Decimal(block[5])
        if block[6] != '':
            v2_enhanced_location.longitude = Decimal(block[6])
        v2_enhanced_location.feature_id = str(block[7])
        v2_enhanced_location.char_offset = int(block[8])

    return v2_enhanced_location.values()


def create_v2_enhanced_locations_array(str_: str):

    array = []
    if str_ != '':
        val = str_.split(';')
        for item in val:
            v2_location = create_v2_enhanced_location(item)
            array.append(v2_location)

    if array: return array



####################################################################################
##################                 V1 PERSON                  ######################
####################################################################################

# 11
def create_v1_persons_array(str_: str) -> Row():

    array = []
    if str_ != '':
        val = str_.split(';')
        for item in val:
            array.append(item)

    if array: return Row(array)



####################################################################################
##################                 V2 PERSON                  ######################
####################################################################################

# 12
def create_enhanced_person(str_: str):

    enhanced_person = EnhancedPerson()
    if str_ != '':
        field = str_.split(',')
        if len(field) == 2:
            enhanced_person.person = str(field[0])
            enhanced_person.char_offset = int(field[1])


    return enhanced_person.values()


def create_v2_enhanced_persons(str_: str):

    array = []
    if str_ != '':
        val = str_.split(';')
        for item in val:
            v2_enhanced_person = create_enhanced_person(item)
            array.append(v2_enhanced_person)

    if array: return array



####################################################################################
##################                    V1 ORG                  ######################
####################################################################################

# 13
def create_v1_orgs(str_: str) -> Row():

    array = []
    if str_ != '':
        val = str_.split(';')
        for item in val:
            array.append(item)

    if array: return Row(array)



####################################################################################
##################                    V2 ORG                  ######################
####################################################################################

# 14
def create_enhanced_org(str_: str):

    org = EnhancedOrg()
    if str_ != '':
        field = str_.split(',')
        if len(field) == 2:
            org.organization = str(field[0])
            org.char_offset = int(field[1])

    return org.values()


def create_v2_enhanced_orgs(str_: str):

    array = []
    if str_ != '':
        val = str_.split(';')
        for item in val:
            v2_enhanced_org = create_enhanced_org(item)
            array.append(v2_enhanced_org)

    if array: return array



####################################################################################
##################                   V15 TONE                  #####################
####################################################################################

# 15
def create_v15_tone(str_: str):

    v15_tone = Tone()
    if str_ != '':
        field = str_.split(',')
        v15_tone.tone = float(field[0])
        v15_tone.positive_score = float(field[1])
        v15_tone.negative_score = float(field[2])
        v15_tone.polarity = float(field[3])
        v15_tone.activity_reference_density = float(field[4])
        v15_tone.self_group_reference_density = float(field[5])
        v15_tone.word_count = int(field[6])

    return v15_tone.values()



####################################################################################
##################             V21 ENHANCED DATES              #####################
####################################################################################

# 16
def create_enhanced_date(str_: str):

    v21_enhanced_date = EnhancedDate()
    field = str_.split('#')
    if len(field) == 5:
        v21_enhanced_date.date_resolution = int(field[0])
        v21_enhanced_date.month = int(field[1])
        v21_enhanced_date.day = int(field[2])
        v21_enhanced_date.year = int(field[3])
        v21_enhanced_date.char_offset = int(field[4])

    return v21_enhanced_date.values()


def create_v21_enhanced_dates(str_: str):

    array = []
    if str_ != '':
        val = str_.split(';')
        for item in val:
            v21_enhanced_date = create_enhanced_date(item)
            array.append(v21_enhanced_date)

    if array: return array



####################################################################################
##################                    V2 GCAM                 #####################
####################################################################################

# 17
def create_gcam(str_: str):

    gcam = Gcam()
    if str_ != '':
        field = str_.split(':')

        gcam.dictionary_dimension_id = str(field[0])
        gcam.score = float(field[1])

    return gcam.values()


def create_v2_gcam(str_: str):

    array = []
    if str_ != '':
        val = str_.split(',')
        for item in val:

            v2_gcam = create_gcam(item)
            array.append(v2_gcam)

    if array: return array 



####################################################################################
##################                V21 SHARE IMAGE              #####################
####################################################################################

# 18
def create_v21_share_img(str_: str):

    if str_ != '':
        img_url = str(str_)

        return Row(img_url)



####################################################################################
##################              V21 RELATED IMAGE              #####################
####################################################################################

# 19
def create_v21_rel_img(str_: str) -> Row():

    array = []
    if str_ != '':
        val = str_.split(';')
        for item in val:
            array.append(item)

    if array: return Row(array)



####################################################################################
##################              V21 SOCIAL IMAGE               #####################
####################################################################################

# 20
def create_v21_soc_img(str_: str) -> Row():

    array = []
    if str_ != '':
        val = str_.split(';')
        for item in val:
            array.append(item)

    if array: return Row(array[:-1])



####################################################################################
##################              V21 SOCIAL VIDEO               #####################
####################################################################################

# 21
def create_v21_soc_vid(str_: str) -> Row():

    array = []
    if str_ != '':
        val = str_.split(';')
        for item in val:
            array.append(item)

    if array: return Row(array[:-1])



####################################################################################
##################                 V21 QUOTES                  #####################
####################################################################################

# 22
def create_quotation(str_: str):

    v21_quote = Quotation()
    field = str_.split('|')
    if len(field) == 4:
        v21_quote.char_offset = int(field[0])
        v21_quote.char_length = int(field[1])
        v21_quote.verb = str(field[2])
        v21_quote.quote = str(field[3])

    return v21_quote.values()


def create_v21_quotes_array(str_: str):

    array = []
    if str_ != '':

        val = str_.split('#')
        for item in val:
            v21_quote = create_quotation(item)
            array.append(v21_quote)

    if array: return array



####################################################################################
##################                 V21 NAMES                   #####################
####################################################################################

# 23
def create_name(str_: str):

    # if str_ != '':
    v21_name = Name()
    if str_ != '':
        field = str_.split(',')
        if len(field) == 2:
            v21_name.name = str(field[0])
            v21_name.char_offset = int(field[1])

    return v21_name.values()


def create_v21_all_names(str_: str):

    array = []
    if str_ != '':
        val = str_.split(';')
        for item in val:
            v21_name = create_name(item)
            array.append(v21_name)

    if array: return array



####################################################################################
##################                 V21 AMOUNTS                 #####################
####################################################################################

# 24
def create_amount(str_: str):

    # if str_ != '':
    v21_amount = Amount()
    if str_ != '':
        field = str_.split(',')
        if len(field) == 3:
            v21_amount.amount = float(field[0])
            v21_amount.object = str(field[1])
            v21_amount.char_offset = int(field[2])

    return v21_amount.values()


def create_v21_amounts(str_: str):

    array = []
    if str_ != '':
        val = str_.split(';')
        for item in val:
            v21_amount = create_amount(item)
            array.append(v21_amount)

    if array: return array[:-1]



####################################################################################
##################            V21 TRANSLATION INFO             #####################
####################################################################################

# 25 
def create_v21_trans_info(str_: str):

    trans_info = TranslationInfo()

    if str_ == '':
        trans_info.srclc = None
        trans_info.eng = None

    if str_ != '':
        field = str_.split(';')
        if len(field) == 2:
            trans_info.srclc = str(f"srclc: {field[0].split(':')[1]}")
            trans_info.eng = str(f"eng: {field[1].split(':')[1]}")


    return trans_info.values()



####################################################################################
##################               V2 EXTRAS XML                 #####################
####################################################################################

# 26 
def create_v2_extras_xml(str_: str):

    extras_xml = V2ExtrasXML()

    if str_ != '':
        # str_.encode('UTF-8').decode('')
        block = str_.strip('<')[0:].strip('>')[:]
        field = block.split('><')
        for val in field:
            if val.startswith('PAGE_TITLE>') and val.endswith('</PAGE_TITLE'):
                extras_xml.title = val.split('>')[1].split('<')[0]
            if val.startswith('PAGE_AUTHORS>') and val.endswith('</PAGE_AUTHORS'):
                extras_xml.authors = val.split('>')[1].split('<')[0]
            if val.startswith('PAGE_LINKS>') and val.endswith('</PAGE_LINKS'):
                extras_xml.links = val.split('>')[1].split('<')[0]
            if val.startswith('PAGE_ALTURL_AMP>') and val.endswith('</PAGE_ALTURL_AMP'):
                extras_xml.alt_url = val.split('>')[1].split('<')[0]
            if val.startswith('PAGE_ALTURL_MOBILE>') and val.endswith('</PAGE_ALTURL_MOBILE'):
                extras_xml.alt_url_mobile = val.split('>')[1].split('<')[0]
            if val.startswith('PAGE_PRECISEPUBTIMESTAMP>') and val.endswith('</PAGE_PRECISEPUBTIMESTAMP'):
                timestamp = val.split('>')[1].split('<')[0]
                extras_xml.pub_timestamp = datetime.datetime.strptime(timestamp, '%Y%m%d%H%M%S')
            if extras_xml.pub_timestamp == '':
                    extras_xml.pub_timestamp = None

    return extras_xml.values()
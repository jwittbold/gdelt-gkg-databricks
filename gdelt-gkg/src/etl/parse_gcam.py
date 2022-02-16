# class for parsing the GCAM codebook
class GcamCodebook:

    def __init__(self):

        self.variable = str('')
        self.dictionary_id = int(0)
        self.dimension_id = int(0)
        self.value_type = str('')
        self.language_code = str('')
        self.dict_human_name = str('')
        self.dim_human_name = str('')
        self.dict_citation = str('')


    def __init__(self, variable, dictionary_id, dimension_id, value_type, language_code, 
            dict_human_name, dim_human_name, dict_citation):

            self.variable = variable
            self.dictionary_id = dictionary_id
            self.dimension_id = dimension_id
            self.value_type = value_type
            self.language_code = language_code
            self.dict_human_name = dict_human_name
            self.dim_human_name = dim_human_name
            self.dict_citation = dict_citation


    def values(self):

        return self.variable, self.dictionary_id, self.dimension_id, self.value_type, self.language_code, \
            self.dict_human_name, self.dim_human_name, self.dict_citation


def gcam_codebook_parser(line):

    val = line.split('\t', -1)
    if len(val) == 8:

        if val != '':

            try:
                gcam_codes = GcamCodebook(variable = str(val[0]),
                                        dictionary_id = int(val[1]),
                                        dimension_id = int(val[2]),
                                        value_type = str(val[3]),
                                        language_code = str(val[4]),
                                        dict_human_name = str(val[5]),
                                        dim_human_name = str(val[6]),
                                        dict_citation = str(val[7])
                                        )
            except Exception as e:
                print(e)
                    
        return gcam_codes.values()
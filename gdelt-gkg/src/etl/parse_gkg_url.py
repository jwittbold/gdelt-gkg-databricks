class GkgUrlParser:

    def __init__(self):

        self.date_time = int(0)
        self.url_suffix = str('')
        self.gkg_url = str('')


    def __init__(self, date_time, url_suffix, gkg_url):

        self.date_time = date_time
        self.url_suffix = url_suffix
        self.gkg_url = gkg_url

    def values(self):

        return self.date_time, self.url_suffix, self.gkg_url

    
def gkg_url_parser(url):

        try:
            parsed_gkg_url = GkgUrlParser(
                            date_time = int(url.split('/')[4].split('.')[0]),
                            url_suffix = str(url[51:]),
                            gkg_url = str(url)
                            )

        except Exception as e:
            print(e)

        return parsed_gkg_url.values()
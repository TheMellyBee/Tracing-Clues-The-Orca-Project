import datetime
import requests
import re
import pandas as pd

from bs4 import BeautifulSoup
from datetime import date
from dateutil import parser


class OrcaNetworkScraper:
    base_url = "http://www.orcanetwork.org/"
    __archive_url = "Archives/index.php?categories_file=Sightings%20Archive%20-%20"
    __userHeader = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 " \
                                  "(KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36"}

    def scrap_archival(self, start_date, end_date):
        if start_date.year < 2001 or end_date > date.today() or start_date > end_date:
            raise ValueError

        timedelta_index = pd.date_range(start=start_date, end=end_date, freq='MS', closed=None).to_series()
        mapFromDateToReport = dict()
        for index, value in timedelta_index.iteritems():
            month = index.to_pydatetime().date()
            mapFromDateToReport.update(self.scrap_archival_page(month))
        return mapFromDateToReport

    def scrap_archival_page(self, archival_date):
        year = archival_date.strftime("%Y")
        month = archival_date.strftime("%b")
        page = requests.get(
            self.base_url + self.__archive_url + month + "%" + year,
            headers=self.__userHeader)
        soup = BeautifulSoup(page.content, 'html.parser')

        divTag = soup.find("div")

        pattern = r"<br>||</br>"
        cleanUpDiv = re.sub(pattern, "", divTag.getText())
        textOnly = re.sub('\n', ' ', cleanUpDiv)
        textOnly = re.sub(
            'Clip Map to enlarge    Map © 2005 used with permission byAdvanced Satellite Productions, Inc.', '',
            textOnly)
        textOnly = textOnly.strip()

        month = archival_date.strftime("%B")
        searchPattern = re.compile(month + " \d{1,2}, \d{4}")
        splitList = re.split(searchPattern, textOnly)
        splitList = splitList[1:]
        mapFromDateToReport = dict()

        for date, text in zip(re.findall(searchPattern, textOnly), splitList):
            dateObject = parser.parse(date).date()
            mapFromDateToReport[dateObject] = [i.strip() for i in text.split('*')]

        return mapFromDateToReport

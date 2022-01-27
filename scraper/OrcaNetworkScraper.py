import datetime
import requests
import re
import pandas as pd
import numpy as np

from bs4 import BeautifulSoup
from datetime import date
from dateutil import parser


base_url = "http://www.orcanetwork.org/"
__archive_url = "Archives/index.php?categories_file=Sightings%20Archive%20-%20"
__userHeader = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 " \
                              "(KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36"}

def scrap_archival(start_date, end_date):
    if start_date.year < 2001 or end_date > date.today() or start_date > end_date:
        raise ValueError

    timedelta_index = pd.date_range(start=start_date, end=end_date, freq='MS', closed=None).to_series()
    mapFromDateToReport = dict()
    for index, value in timedelta_index.iteritems():
        month = index.to_pydatetime().date()
        mapFromDateToReport.update(scrap_archival_page(month))
    return mapFromDateToReport



def scrap_archival_page(archival_date):
    year = archival_date.strftime("%Y")
    month = archival_date.strftime("%b")
    page = requests.get(
        base_url + __archive_url + month + "%" + year,
        headers=__userHeader)
    soup = BeautifulSoup(page.content, 'html.parser')

    divTag = soup.find("div")

    pattern = r"<br>||</br>||<b>||</br>||<center>||</center>||<font.*>||</font>"
    cleanUpDiv = re.sub(pattern, "", divTag.getText())
    textOnly = re.sub('\n', ' ', cleanUpDiv)
    textOnly = re.sub("'", "\'", textOnly)
    textOnly = re.sub('"', '\\"', textOnly)
    textOnly = re.sub(
        "Click Map to enlarge   Map .* used with permission byAdvanced Satellite Productions, Inc.", '',
        textOnly)
    textOnly = re.sub(
        "Click here for Map of .* whale sightings.", '',
        textOnly)
    textOnly = textOnly.strip()

    month = archival_date.strftime("%B")
    year = archival_date.strftime("%Y")
    searchPattern = re.compile(month + " \d{1,2}")
    splitList = re.split(searchPattern, textOnly)
    splitList = splitList[1:]
    mapFromDateToReport = dict()


    datelist = re.findall(searchPattern, textOnly)
    datelist = [date + ", " + year for date in datelist]
    unquieDates = np.unique(np.array(datelist)).tolist()
    parsedDates = [parser.parse(uniqueDate).date() for uniqueDate in unquieDates]
    parsedDates.sort()

    for dateObject, text in zip(parsedDates, splitList):
        mapFromDateToReport[dateObject] = [i.strip() for i in text.split('*')]

    return mapFromDateToReport


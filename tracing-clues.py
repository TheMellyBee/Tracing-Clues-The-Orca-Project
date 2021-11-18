from dateutil import parser
from scraper.OrcaNetworkScraper import OrcaNetworkScraper


def test_archival_page():
    start_date = parser.parse("October 1, 2005").date()
    end_date = parser.parse("December 1, 2005").date()

    web_scrapper = OrcaNetworkScraper()
    map_date_to_reports = web_scrapper.scrap_archival(start_date, end_date)
    print(sorted(map_date_to_reports.keys()))


if __name__ == '__main__':
    test_archival_page()

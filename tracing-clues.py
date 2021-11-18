from dateutil import parser

from graph_lib.add_report_and_date import create_date_and_report
from scraper import OrcaNetworkScraper
from graph_lib import credentials
from graph_lib.neo4jConnection import Neo4jConnection


def main():
    start_date = parser.parse("January 1, 2002").date()
    end_date = parser.parse("October 1, 2021").date()

    print("Scrapping Orca Network for dates from {} to {}".format(start_date, end_date))
    map_date_to_reports = OrcaNetworkScraper.scrap_archival(start_date, end_date)
    print("Scrapping finished. Number of dates: " + str(len(map_date_to_reports)))

    print("Opening connection to Tracing-Clues' Orca Graph")
    connection = Neo4jConnection(credentials.__uri, credentials.__user, credentials.__password)
    print("Connection established at: " + str(connection))

    print("Creating Reports")
    try:
       create_date_and_report(connection, map_date_to_reports)
    except Exception as e:
        print("Query failed:", e)
        print("\tuh oh... connection not made")
    print("Reports complete.")

    print("Closing Connection")
    connection.close()


if __name__ == '__main__':
    main()

from dateutil import parser

from graph_lib.add_report_and_date import create_date_and_report
from scraper import OrcaNetworkScraper
from graph_lib import credentials
from graph_lib.neo4jConnection import Neo4jConnection
from graph_lib.add_births_and_deaths import import_births
from graph_lib.add_births_and_deaths import import_pods_birth
from graph_lib.add_births_and_deaths import import_deaths


def create_pods():
    print("Opening connection to Tracing-Clues' Orca Graph")
    connection = Neo4jConnection(credentials.__uri, credentials.__user, credentials.__password)
    print("Connection established at: " + str(connection))

    print("Importing births...")
    import_births(connection)
    print("Major set up done,")

    print("Importing more births...")
    import_pods_birth(connection)
    print("All births imported.")

    print("Importing recorded deaths..")
    import_deaths(connection)
    print("Importing deaths finished.")

    print("Imports complete.")

    print("Closing Connection")
    connection.close()


def create_database_from_archives():
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


def main():
    # create_database_from_archives()
    # create_pods():
    pass


if __name__ == '__main__':
    main()

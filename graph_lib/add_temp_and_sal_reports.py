import pandas as pd
from dateutil import parser
from graph_lib.add_report_and_date import add_time_line
from graph_lib.location_queries import set_location


def import_sal_and_temp(connection):
    print("Reading in temp and sal CSV...")
    temp_sal_df = pd.read_csv("data/temp_sal/temp_sal.csv",
                              skipinitialspace=True,
                              na_values='nd',
                              date_parser=pd.to_datetime,
                              parse_dates=['Date', 'ISO_DateTime_UTC']
                              )

    print("CSV loaded. Building Query...")

    # clean data
    temp_sal_df.dropna(subset=['Temperature', 'Salinity'], how='all')

    # Create the query
    data_set_title = create_uw_citation(connection)

    batching = 0
    for index, row in temp_sal_df.iterrows():
        create_temp_sal_query(connection, row, data_set_title)
        batching += 1

        if batching % 10 == 0:
            print("10 queries finished.")

    print("All queries finished.")


def create_uw_citation(connection):
    query_builder = ''

    # Organization
    query_builder += "MERGE (uw:Organization {name:'University of Washington'})\n"
    query_builder += "MERGE (lab:Organization {name:'Friday Harbor Laboratories'," + \
                     "url:'https://fhl.uw.edu'})\n"

    # Author
    query_builder += "MERGE (p:Person {name:'Emily Carrington'," + \
                     "orcidId:'0000-0001-8741-4828'})\n"

    # Dataset info
    query_builder += "MERGE (data:DataSet {title:'UW FHL Temperature & Salinity', " + \
                     "url:'https://www.bco-dmo.org/dataset/775732/data'" + \
                     "version:1, " + \
                     "project:'Effects of Ocean Acidification on Coastal Organisms: An " \
                     "Ecomaterials Perspective', " + \
                     "abstract:'Hourly seawater temperature and salinity values taken by an " \
                     "SBE 37 MicroCAT at UW FHL (University of Washington, Friday Harbor " \
                     "Laboratories) in Friday Harbor between January 1, 2010 and January 1, " \
                     "2016.'," + \
                     "citation:'Carrington, E. (2019) UW FHL Temperature & Salinity data taken " \
                     "at Friday Harbor, WA between between January 1, 2010 and January 1, " \
                     "2016. Biological and Chemical Oceanography Data Management Office (" \
                     "BCO-DMO). (Version 1) Version Date 2019-08-20 [if applicable, " \
                     "indicate subset used]. doi:10.1575/1912/bco-dmo.775732.1'})\n"

    # Connect them
    query_builder += "MERGE (p)-[:AFFILIATED_WITH]->(uw)\n"
    query_builder += "MERGE (lab)-[:AFFILIATED_WITH]->(uw)\n"
    query_builder += "MERGE (p)<-[:PRIMARY_INVESTIGATOR]-(data)\n"
    query_builder += "MERGE (lab)<-[:GATHERED_AT]-(data)\n"

    # Submit query
    connection.query(query_builder)

    # return data set title
    return "UW FHL Temperature & Salinity"


def create_temp_sal_query(connection, row, data_set_title):
    query_builder = ""

    # merge a date ontology
    measurement_date = parser.parse(row['Date']).date()
    add_time_line(connection, measurement_date)
    query_builder += "MATCH (day:Day {date: '%s'})\n" % (measurement_date.strftime("%B %d, %Y"))

    # connect location
    address = set_location(connection, (row['Latitude'], row['Longitude']))
    query_builder += "MATCH (loc:Location {address: '%s'}\n" % address

    # Get the data set
    query_builder += "MATCH (data:DataSet {title:'%s'})\n" % data_set_title

    # create source of report and organization
    query_builder += "WITH day, loc, data\n"

    # create the measurement node
    query_builder += "CREATE (m:Measurement)\n"

    # create nodes for the different tests (this is for easier querying
    if not pd.isnull(row["Temperature"]):
        query_builder += "CREATE (temp:WaterTemperature {value:%s, unit:'%s'})\n" % (row["Temperature"], "C")
    elif not pd.isnull(row["Temperature"]):
        query_builder += "CREATE (sal:Salinity {value:%s, unit:'%s'})\n" % (row["Salinity"], "PSU")

    # connect nodes
    query_builder += "MERGE (m)-[:TAKEN_ON]->(day)\n"
    query_builder += "MERGE (m)-[:TAKEN_AT]->(loc)\n"
    query_builder += "MERGE (m)-[:ELEMENT_OF]->(data)\n"
    query_builder += "MERGE (m)-[:COLLECTS]->(temp)\n"
    query_builder += "MERGE (m)-[:COLLECTS]->(sal)\n"

    # send the query
    connection.query(query_builder)

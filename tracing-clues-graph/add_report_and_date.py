from neo4jConnection import  Neo4jConnection
from datetime import date

def add_time_line(connection ,curr_date):
    year = curr_date.strftime("%Y")
    month = curr_date.strftime("%B")
    month_number = int(curr_date.strftime("%m"))
    year_month = curr_date.strftime("%B %Y")
    day = int(curr_date.strftime("%d"))
    day_name = curr_date.strftime("%A")
    full_date = curr_date.strftime("%B %d, %Y")

    query = '''
                MERGE (y:Year {date: '%s'})
                MERGE (m:Month {date: '%s', month: '%s', number: %d})
                MERGE (d:Day {date: '%s', weekday: '%s', number: %d})
                CREATE (d)-[:DAY_IN]->(m)-[:MONTH_IN]->(y)
             ''' % (year, year_month, month, month_number, full_date, day_name, day)
    connection.query(query)

def add_report(connection, date, report):
    full_date = date.strftime("%B %d, %Y")
    query = '''
                MATCH (d:Day {date: '%s'})
                MERGE (s:Sighting {full_text: '%s', processed: 'no'})
                CREATE (s)-[:ON]->(d)
            ''' %full_date, report
    connection.query(query)

def create_date_and_report(connection, map_date_to_report):
    for date, report_list in map_date_to_report.items():
        add_time_line(connection, date)
        for report in report_list:
            add_report(connection, date, report)

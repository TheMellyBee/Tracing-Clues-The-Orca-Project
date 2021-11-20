import pandas
from datetime import date
from dateutil import parser
from graph_lib.add_report_and_date import add_time_line


def import_births(connection):
    print("Reading in orca birth CSV..")
    orca_births_df = pandas.read_csv("data/orca_birth.csv")

    print("CSV loaded. Building query..")
    batching = 0
    for index, row in orca_births_df.iterrows():
        __create_birth_query_from_row(connection, index, row)
        batching += 1

        if batching % 10 == 0:
            print("10 queries finished.")

    print("All queries finished.")

def __create_birth_query_from_row(connection, index, row):
    # Create the child orca
    query_builder = "MERGE (c:Orca {name:'%s'})\n" % (index.strip())

    if row['orca_name'] != 'None':
        query_builder += "SET c.nick_name = '%s'\n" % (row['orca_name'].strip())
    if row['gender'] != 'None':
        query_builder += "SET c.gender = '%s'\n" % (row['gender'].strip())

    # Create birth event
    query_builder += "CREATE (be:Birth :Event)\n"

    # Get the mother
    query_builder += "MERGE (m:Orca {name:'%s', gender:'Female'})\n" % (row['mother'].strip())

    if row['mother_name'] != 'None':
        query_builder += "SET m.nick_name = '%s'\n" % (row['mother_name'].strip())
    query_builder += "WITH c, m, be\n"

    # birth date
    birth_date = parser.parse(row['birth_date']).date()
    add_time_line(connection, birth_date)
    query_builder += "MATCH (d:Day {date: '%s'})\n" % (birth_date.strftime("%B %d, %Y"))


    # Make the relationships
    query_builder += "MERGE (m)-[:CALVES {calf_number:%d}]->(be)\n" % (int(row['calf_number']))
    query_builder += "MERGE  (d)<-[:BORN_ON]-(be)-[:CALF]->(c)"

    connection.query(query_builder)


def import_pod_birth(connection, pod_name):
    pod_name = pod_name.lower()
    current_implementation = {'j', 'k', 'l1', 'l2'}

    if pod_name not in current_implementation:
        raise NotImplementedError

    print("Loading in" + pod_name + "Pod specific csv...")
    pod_birth_df = pandas.read_csv("data/" + pod_name + "pod_births.csv")

    print("Creating "+ pod_name +"Pod...")
    pod_birth_df['sex'] = pod_birth_df['sex'].apply(__change_sex_to_word)
    query_builder = "MERGE (%s:Pod {name:'%s'}) "%(pod_name, pod_name.upper())
    for index, row in pod_birth_df.iterrows():
        query_builder += """
                            MERGE (%s:Orca {name:'%s', gender:'%s'})
                            CREATE (b%s:Birth :Event)
                            MERGE (y%s:Year {date:'%s'})
                            MERGE (y%s)<-[:BORN_ON]-(b%s)-[:CALF]->(%s)-[:MEMBER_OF]-(%s)
                         """ % (row['name'], row['name'], row['sex'],row['name'], row['name'], row['DOB'], row['name'],row['name'], row['name'], pod_name)

    print("Pod query built.")

    print("Submit Query using Connection: " + str(connection))
    connection.query(query_builder)
    print("Query completed.")


def import_pods_birth(connection):
    print("Starting pod loading...")
    import_pod_birth(connection, 'j')
    import_pod_birth(connection, 'k')
    import_pod_birth(connection, 'l1')
    import_pod_birth(connection, 'l2')
    print("All pods completed.")


def __change_sex_to_word(sex):
    if sex == 'M':
        sex = "Male"
    elif sex == 'F':
        sex = "Female"
    return sex


def import_deaths(connection):
    print("Loading in csv...")
    orca_death_df = pandas.read_csv("data/orca_death.csv")
    print("CSV Loaded.")

    print("Beginning creating queries...")
    batching = 0
    for index, row in orca_death_df.iterrows():
        __create_death_query_from_row(connection, index, row)
        batching += 1

        if batching % 10 == 0:
            print("10 queries completed.")

    print("All death imports complete!")

def __create_death_query_from_row(connection, index, row):
    # Create/Match the Orca
    query_builder = "MERGE (o:Orca {name:'%s'})\n" % (index.strip())
    if row['name'] != 'None':
        query_builder += "SET o.nick_name = '%s'\n" % (row['name'].strip())
    if row['gender'] != 'None':
        query_builder += "SET o.gender = '%s'\n" % (row['gender'].strip())
    query_builder += 'WITH o\n'

    # Create a birth event
    query_builder += "CREATE (be:Birth :Event)\n"
    query_builder += "MERGE (o)<-[:CALF]-(be)\n"

    # Create/Match Mother
    if row['mother'] != "None":
        query_builder += "MERGE (m:Orca {name:'%s', gender:'Female'})\n" % (row['mother'].strip())
        if row['mother_name'] != "None":
            query_builder += "SET m.nick_name = '%s'\n" % (row['mother_name'].strip())
            query_builder += 'WITH o, m\n'
        query_builder += "MERGE (m)-[:CALVES]->(be)\n"

    # Add birthdate
    if row['birth_date'] != 'None':
        birth_date = parser.parse(row['birth_date']).date()
        # This is going to parse, but if it's in January just do the year
        if birth_date.month == 1:
            query_builder += "MERGE (d:Year {date:'%s'})\n" % (birth_date.strftime("%Y"))
        else:
            add_time_line(connection, birth_date)
            query_builder += "WITH be, o\n"
            query_builder += "MATCH (d:Day {date: '%s'})\n" % (birth_date.strftime("%B %d, %Y"))
        query_builder += "MERGE (be)-[:BORN_ON]->(d)\n"

    # Create death event with added label
    if 'found' in row['orca_status'] or 'washed' in row['orca_status']:
        query_builder += "MERGE (de:Death :Event :FoundDead {status:'%s', reported_age:'%s'})\n" % (row['orca_status'], row['age'])
    elif 'missing' in row['orca_status'] or 'last seen' in row['orca_status']:
        query_builder += "MERGE (de:Death :Event :Missing {status:'%s', reported_age:'%s'})\n" % (row['orca_status'], row['age'])
    else:
        query_builder += "MERGE (de:Death :Event {status:'%s', reported_age:'%s'})\n" % (row['orca_status'], row['age'])
    query_builder += "MERGE (o)-[:DIED]->(de)\n"

    # create death date
    death_date = parser.parse(row['date']).date()
    add_time_line(connection, death_date)
    query_builder += "WITH de\n"
    query_builder += "MATCH (d:Day {date: '%s'})\n" % (death_date.strftime("%B %d, %Y"))
    query_builder += "MERGE (de)-[:ON]->(d)\n"

    # Point to a location found
    if row['location'] != 'None':
        query_builder += "MERGE (l:Location {text:'%s'})\n" % (row['location'])
        query_builder += "MERGE (de)-[:AT]->(l)\n\n"

    connection.query(query_builder)

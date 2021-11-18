from neo4jConnection import Neo4jConnection

class OrcaGraphConnection:
    __uri = 'neo4j+s://b701f78d.databases.neo4j.io'
    __user = ''
    __password = ''

    def get_connection():
        return Neo4jConnection(uri=OrcaGraphConnection.__uri,
                               user=OrcaGraphConnection.__user,
                               pwd=OrcaGraphConnection.__password)

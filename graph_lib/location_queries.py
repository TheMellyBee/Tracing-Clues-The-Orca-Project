from geopy.geocoders import Nominatim


def get_geolocater():
    geolocator = Nominatim(user_agent="tracing_clues")
    return geolocator


def set_location(connection, coordinates):
    geolocator = get_geolocater()
    location = geolocator.geocode(coordinates)

    query = "MERGE (l:Location" + location.raw['address'] + ')'
    query += "set l.latitude = " + location.latitude
    query += "set l.longitude = " + location.longitude
    connection.query(query)
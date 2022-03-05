from geopy.geocoders import Nominatim


def get_geolocater():
    geolocator = Nominatim(user_agent="tracing_clues")
    return geolocator


def set_location(connection, coordinates):
    geolocator = get_geolocater()
    location = geolocator.geocode(coordinates)

    query = "MERGE (l:Location {address:'%s'\n" % (location.raw['address'])
    query += "set l.latitude = " + location.latitude + "\n"
    query += "set l.longitude = " + location.longitude + "\n"
    connection.query(query)
    return location.raw['address']
from geopy.geocoders import Nominatim


def get_geolocater():
    geolocator = Nominatim(user_agent="tracing_clues")
    return geolocator


def set_location(connection, lat, long):
    geolocator = get_geolocater()
    cordinates = str(lat) + "," + str(long)
    location = geolocator.reverse(cordinates)

    query = "MERGE (l:Location {address:'%s'})\n" % (location.address)
    query += "set l.latitude = " + str(lat) + "\n"
    query += "set l.longitude = " + str(long) + "\n"
    connection.query(query)
    return location.address
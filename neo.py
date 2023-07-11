import itertools
from pymongo import MongoClient
from py2neo import Graph, Node, Relationship

# Establecer la conexión con MongoDB
client = MongoClient('mongodb://localhost:27017')
db = client["proyecto"]
collection = db["proyecto"]
passengers = db["passangers"]
countries = db["countries"]

# Establecer la conexión con Neo4j
neo4j_graph = Graph("bolt://localhost:7687", auth=("neo4j", "12345678"))

# Obtener todos los elementos de la colección de reservas en MongoDB
results = collection.find()

# Obtener todos los elementos de la colección de pasajeros en MongoDB
passenger_results = passengers.find()

# Obtener todos los elementos de la colección de países en MongoDB
countries_results = countries.find()

# Obtener el número de documentos en la colección de reservas
booking_document_count = collection.count_documents({})

# Obtener el número de documentos en la colección de pasajeros
passenger_document_count = passengers.count_documents({})

# Obtener el número de países en la colección de países
countries_document_count = countries.count_documents({})

# Imprimir el número de documentos de reservas, pasajeros y países
print(f"La colección de reservas tiene {booking_document_count} documentos.")
print(f"La colección de pasajeros tiene {passenger_document_count} documentos.")
print(f"La colección de países tiene {countries_document_count} documentos.")

a_results=itertools.islice(passenger_results,50)
b_results=itertools.islice(results,50)
c_results=itertools.islice(countries_results,50)


# Crear nodos de países en Neo4j
for country in c_results:
    country_node = Node("Countries",
                          country_id=country["country_id"],
                          country=country["country"])
    neo4j_graph.create(country_node)

# Crear nodos de pasajeros en Neo4j y establecer relaciones con los países
for passenger in a_results:
    passenger_node = Node("Passenger",
                          passenger_id=passenger["passenger_id"],
                          name=passenger["name"],
                          age=passenger["age"],
                          gender=passenger["gender"],
                          nationality=passenger["nationality"],
                          email=passenger["email"],
                          country_id=passenger["country_id"])
    neo4j_graph.create(passenger_node)

    # Obtener el nodo de país correspondiente en Neo4j
    country_id = passenger["country_id"]
    country_node = neo4j_graph.nodes.match("Countries", country_id=country_id).first()

    # Establecer la relación entre el nodo de pasajero y el nodo de país
    relationship = Relationship(passenger_node, "COUNTRY", country_node)
    neo4j_graph.create(relationship)

# Crear nodos de reservas en Neo4j y establecer relaciones con los pasajeros
for result in b_results:
    # Crear un nodo de reserva en Neo4j
    booking_node = Node("Booking",
                        booking_id=result["booking_id"],
                        num_passengers=result["num_passengers"],
                        sales_channel=result["sales_channel"],
                        trip_type=result["trip_type"],
                        purchase_lead=result["purchase_lead"],
                        length_of_stay=result["length_of_stay"],
                        flight_hour=result["flight_hour"],
                        flight_day=result["flight_day"],
                        route=result["route"],
                        booking_origin=result["booking_origin"],
                        wants_extra_baggage=result["wants_extra_baggage"],
                        want_preferred_seat=result["want_preferred_seat"],
                        wants_in_flight_meals=result["wants_in_flight_meals"],
                        flight_duration=result["flight_duration"],
                        booking_complete=result["booking_complete"])
    neo4j_graph.create(booking_node)

    # Obtener el nodo de pasajero correspondiente en Neo4j
    passenger_id = result["passenger_id"]
    passenger_node = neo4j_graph.nodes.match("Passenger", passenger_id=passenger_id).first()

    # Establecer la relación entre el nodo de pasajero y el nodo de reserva
    relationship = Relationship(passenger_node, "BOOKED", booking_node)
    neo4j_graph.create(relationship)

# Cerrar la conexión con Neo4j (automáticamente cierra la conexión con MongoDB)
client.close()

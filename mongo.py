import csv
from pymongo import MongoClient

MONGO_URL = "mongodb://localhost:27017"
DB_NAME = "proyecto"
COLLECTION_NAME = "proyecto"
COUNTRIES_COLLECTION_NAME = "countries"
PASSENGERS_COLLECTION_NAME = "passangers"
CSV_FILE = "/Users/josuecarpio/Downloads/bd2 2/Passanger_booking_data .csv"


def insert_passenger_documents(passengers, collection):
    with open(CSV_FILE, "r") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            passenger_id = int(row['passenger_id'])
            booking_ids = passengers.get(passenger_id, [])
            # Obtener los datos adicionales del pasajero del CSV
            passenger_data = {
                "name": row['name'],
                "age": int(row['age']),
                "gender": row['gender'],
                "nationality": row['nationality'],
                "email": row['email'],
                "country_id": row['country_id']
            }
            document = {
                "passenger_id": passenger_id,
                "booking_ids": booking_ids,
                **passenger_data
            }
            collection.insert_one(document)
            
def create_passenger_list(file):
    passengers = {}
    with open(file, "r") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            passenger_id = int(row['passenger_id'])
            booking_id = int(row['booking_id'])
            if passenger_id in passengers:
                passengers[passenger_id].append(booking_id)
            else:
                passengers[passenger_id] = [booking_id]
    return passengers

def insert_documents(passengers, collection):
    with open(CSV_FILE, "r") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            booking_id = int(row['booking_id'])
            passenger_id = int(row['passenger_id'])
            document = {
                "booking_id": booking_id,
                "passenger_id": passenger_id,
                "num_passengers": int(row['num_passengers']),
                "sales_channel": row['sales_channel'],
                "trip_type": row['trip_type'],
                "purchase_lead": row['purchase_lead'],
                "length_of_stay": int(row['length_of_stay']),
                "flight_hour": row['flight_hour'],
                "flight_day": row['flight_day'],
                "route": row['route'],
                "booking_origin": row['booking_origin'],
                "wants_extra_baggage": bool(row['wants_extra_baggage']),
                "want_preferred_seat": row['wants_preferred_seat'],
                "wants_in_flight_meals": bool(row['wants_in_flight_meals']),
                "flight_duration": float(row['flight_duration']),
                "booking_complete": int(row['booking_complete'])
            }
            collection.insert_one(document)

def main():
    client = MongoClient(MONGO_URL)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    countries_collection = db[COUNTRIES_COLLECTION_NAME]
    passangers = db[PASSENGERS_COLLECTION_NAME]

    # Eliminar todos los documentos existentes en la colección
    collection.delete_many({})
    passangers.delete_many({})
    countries_collection.delete_many({})

    # Crear la lista de pasajeros
    passengers = create_passenger_list(CSV_FILE)

    # Insertar los documentos de reserva y establecer las relaciones con los pasajeros
    insert_documents(passengers, collection)
    
    # Crear la colección de países
    
    # Insertar los documentos de países en la colección de países
    with open(CSV_FILE, "r") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            document = {
                "country_id": row['country_id'],
                "country": row['nationality']
            }
            if not countries_collection.find_one(document):
                countries_collection.insert_one(document)
            
    collection = db["passangers"]
    insert_passenger_documents(passengers, collection)

    # Cerrar la conexión con MongoDB
    client.close()
    print("pasa el papel")


if __name__ == "__main__":
    main()

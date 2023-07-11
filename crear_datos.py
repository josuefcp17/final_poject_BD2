from faker import Faker
import random

fake = Faker()

data = []

for _ in range(10000):
    booking_id = fake.random_int(min=1, max=999999)
    passenger_id = fake.random_int(min=1, max=999999)
    num_passengers = random.randint(1, 5)
    sales_channel = random.choice(["Online", "Offline"])
    trip_type = random.choice(["One-way", "Round-trip"])
    purchase_lead = random.randint(1, 60)
    length_of_stay = random.randint(1, 10)
    flight_hour = fake.time(pattern="%H:%M")
    flight_day = fake.date_between(start_date='-30d', end_date='+30d')
    route = fake.random_element(["New York - London", "Paris - Rome", "Tokyo - Sydney"])
    booking_origin = fake.random_element(["JFK", "CDG", "NRT"])
    wants_extra_baggage = random.choice(["Yes", "No"])
    wants_preferred_seat = random.choice(["Yes", "No"])
    wants_in_flight_meals = random.choice(["Yes", "No"])
    flight_duration = random.uniform(1, 12)
    booking_complete = random.randint(0, 1)
    name = fake.name()
    age = random.randint(18, 80)
    gender = random.choice(["Male", "Female"])
    nationality = fake.country()
    email = fake.email()
    country_id = fake.random_int(min=1, max=10)

    data.append((booking_id, passenger_id, num_passengers, sales_channel, trip_type, purchase_lead,
                 length_of_stay, flight_hour, flight_day, route, booking_origin, wants_extra_baggage,
                 wants_preferred_seat, wants_in_flight_meals, flight_duration, booking_complete, name,
                 age, gender, nationality, email, country_id))

# Guardar los datos en un archivo CSV
import csv

header = ["booking_id", "passenger_id", "num_passengers", "sales_channel", "trip_type", "purchase_lead",
          "length_of_stay", "flight_hour", "flight_day", "route", "booking_origin", "wants_extra_baggage",
          "wants_preferred_seat", "wants_in_flight_meals", "flight_duration", "booking_complete", "name",
          "age", "gender", "nationality", "email", "country_id"]

with open('datos.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(header)
    writer.writerows(data)
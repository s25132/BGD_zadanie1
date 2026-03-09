import csv
import os
import random
from datetime import datetime, timedelta

OUTPUT_FILE = os.getenv("OUTPUT_FILE", "transactions.csv")
ROWS = int(os.getenv("FILES_TO_GENERATE", "1000000"))

first_names = ["Jan", "Anna", "Piotr", "Kasia", "Tomasz", "Maria", "Adam", "Ewa"]
last_names = ["Nowak", "Kowalski", "Wisniewski", "Wojcik", "Kaczmarek", "Mazur"]

cities = ["Warsaw", "Krakow", "Gdansk", "Wroclaw", "Poznan"]
countries = ["PL", "DE", "FR", "ES", "IT", "Polska"]
payment_methods = ["card", "blik", "transfer", "cash", "CARD"]
statuses = ["approved", "declined", "pending", "APPROVED"]

start_date = datetime(2023, 1, 1)

with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow([
        "transaction_id",
        "customer_id",
        "customer_name",
        "merchant_id",
        "transaction_ts",
        "amount",
        "city",
        "country",
        "payment_method",
        "status"
    ])

    for i in range(ROWS):
        dt = start_date + timedelta(minutes=random.randint(0, 1_500_000))
        amount = round(random.uniform(10, 10000), 2)

        if i % 10000 == 0:
            amount_value = "BAD_AMOUNT"
        else:
            amount_value = amount

        if i % 15000 == 0:
            ts_value = "wrong_date"
        else:
            ts_value = dt.strftime("%Y-%m-%d %H:%M:%S")

        if i % 20000 == 0:
            transaction_id = ""
        else:
            transaction_id = f"T{i}"

        customer_id = f"C{random.randint(1, 500000)}"
        customer_name = f"{random.choice(first_names)} {random.choice(last_names)}"

        writer.writerow([
            transaction_id,
            customer_id,
            customer_name,
            f"M{random.randint(1, 50000)}",
            ts_value,
            amount_value,
            random.choice(cities),
            random.choice(countries),
            random.choice(payment_methods),
            random.choice(statuses)
        ])

print(f"Wygenerowano plik: {OUTPUT_FILE}")
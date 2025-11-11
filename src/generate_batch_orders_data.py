import csv, random, uuid, time, datetime

OUTPUT_FILE = "historical_orders.csv"
PRODUCT_IDS = list(range(1, 21))
USER_IDS = list(range(1001, 1101))

def generate_row(order_date):
    product_id = random.choice(PRODUCT_IDS)
    quantity = random.randint(1, 5)
    price = round(random.uniform(10, 200), 2)
    return {
        "order_id": str(uuid.uuid4()),
        "user_id": random.choice(USER_IDS),
        "product_id": product_id,
        "quantity": quantity,
        "price": price,
        "order_date": order_date.strftime("%Y-%m-%d %H:%M:%S")
    }

with open(OUTPUT_FILE, "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=["order_id","user_id","product_id","quantity","price","order_date"])
    writer.writeheader()
    start = datetime.datetime.now() - datetime.timedelta(days=180)
    for i in range(10000):  # 10K dummy rows
        writer.writerow(generate_row(start + datetime.timedelta(minutes=i)))
print("✅ Generated:", OUTPUT_FILE)
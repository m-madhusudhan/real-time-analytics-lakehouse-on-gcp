import json, time, uuid, requests
from google.cloud import pubsub_v1

project_id = "qwiklabs-gcp-01-cffc452c1796"
topic_id   = "orders-topic"
publisher  = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

def fetch_orders():
    r = requests.get("https://fakestoreapi.com/carts")
    data = r.json()
    for cart in data:
        user_id = cart["userId"]
        for p in cart["products"]:
            yield {
                "order_id": str(uuid.uuid4()),
                "user_id": user_id,
                "product_id": p["productId"],
                "quantity": p["quantity"],
                "price": round(10 + 90 * p["quantity"], 2),
                "timestamp": time.time()
            }

while True:
    for order in fetch_orders():
        publisher.publish(topic_path, json.dumps(order).encode("utf-8"))
    print("Published batch of orders")
    time.sleep(10)


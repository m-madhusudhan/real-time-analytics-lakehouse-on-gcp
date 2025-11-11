import json, time, uuid, random
from google.cloud import pubsub_v1

project_id = "qwiklabs-gcp-01-cffc452c1796"
topic_id   = "inventory-topic"
publisher  = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

PRODUCT_IDS = list(range(1, 21))           # assume 20 products
WAREHOUSES  = ["W1", "W2", "W3"]

def generate_inventory_record():
    return {
        "inventory_id": str(uuid.uuid4()),
        "product_id": random.choice(PRODUCT_IDS),
        "warehouse_id": random.choice(WAREHOUSES),
        "quantity_in_stock": random.randint(0, 500),
        "last_updated": time.time()
    }

while True:
    for _ in range(10):   # send 10 inventory messages each cycle
        msg = json.dumps(generate_inventory_record()).encode("utf-8")
        publisher.publish(topic_path, msg)
    print("Published inventory updates")
    time.sleep(15)

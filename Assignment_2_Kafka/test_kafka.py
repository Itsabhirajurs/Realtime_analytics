import json
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
)
future = producer.send("sensor_data", {"time": 1.0, "acc_x": 0.1, "acc_y": 0.2, "acc_z": 9.8})
print("Delivered:", future.get(timeout=10))
producer.flush()
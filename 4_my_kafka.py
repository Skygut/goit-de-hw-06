from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "alert_Kafka_topic",
    bootstrap_servers="77.81.230.104:9092",
    security_protocol="SASL_PLAINTEXT",
    sasl_mechanism="PLAIN",
    sasl_plain_username="admin",
    sasl_plain_password="VawEzo1ikLtrA8Ug8THa",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="alert_consumer_group",
)

print("Починаємо читання повідомлень з топіка...")
for message in consumer:
    print(f"Отримано повідомлення: {message.value.decode('utf-8')}")

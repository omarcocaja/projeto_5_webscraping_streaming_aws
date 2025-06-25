from kafka import KafkaProducer
import json

def write_to_kafka(broker_url: str, topic: str, record: dict):
    topic = topic.lower() # Evita duplicidade lógica (Reddit != reddit)

    producer = KafkaProducer(
        bootstrap_servers=[broker_url],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: str(k).encode('utf-8') if k else None
    )

    try:
        producer.send(topic, value=record)
        producer.flush()
    except Exception as e:
        print(f"Erro ao enviar para Kafka: {e}")
    finally:
        producer.close()

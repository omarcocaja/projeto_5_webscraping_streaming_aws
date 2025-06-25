import os
import json
import time
import boto3
from kafka import KafkaConsumer
from datetime import datetime
from io import BytesIO

# Variáveis de ambiente
AWS_ACCESS_KEY_ID = os.getenv("AWS_SECRET_KEY")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_VALUE")
AWS_REGION = "us-east-2"
BUCKET = "portfolio-projeto-cinco"
S3_PREFIX = "datalake/landing/source=emsc"

# Sessão boto3 autenticada
session = boto3.Session(
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION
)
s3 = session.client("s3")

# Kafka Consumer
consumer = KafkaConsumer(
    "emsc",
    bootstrap_servers=["localhost:9092"],
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id="emsc_s3_json_writer"
)

def write_json_to_s3(event):
    try:
        timestamp = datetime.utcnow()
        date_str = timestamp.strftime("%Y-%m-%d")
        filename = f"emsc_{timestamp.strftime('%Y%m%d_%H%M%S_%f')}.json"
        key = f"{S3_PREFIX}/date={date_str}/{filename}"

        # Converte para JSON e escreve no buffer
        buffer = BytesIO()
        buffer.write(json.dumps(event).encode("utf-8"))
        buffer.seek(0)

        s3.upload_fileobj(buffer, Bucket=BUCKET, Key=key)
        print(f"Evento salvo em: s3://{BUCKET}/{key}")
    except Exception as e:
        print(f"Erro ao salvar evento: {e}")

def main():
    print("Aguardando mensagens do tópico 'emsc'...")
    try:
        for message in consumer:
            write_json_to_s3(message.value)
            time.sleep(1)
    except KeyboardInterrupt:
        print("Encerrado manualmente.")
    except Exception as e:
        print(f"Erro inesperado: {e}")

if __name__ == "__main__":
    main()

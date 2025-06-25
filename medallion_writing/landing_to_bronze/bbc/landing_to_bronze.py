import boto3
import pandas as pd
import json
import io
import os
from datetime import datetime, timezone

bucket = "portfolio-projeto-cinco"
landing_prefix = "datalake/landing/bbc/"
bronze_prefix = "datalake/bronze/"
log_prefix = "datalake/logs/bronze_ingest_log/"
checkpoint_key = "datalake-checkpoints/bbc_landing.json"

s3 = boto3.client("s3")

def load_checkpoint():
    try:
        obj = s3.get_object(Bucket=bucket, Key=checkpoint_key)
        content = obj["Body"].read().decode("utf-8")
        data = json.loads(content)
        return datetime.fromisoformat(data.get("last_modified"))
    except s3.exceptions.NoSuchKey:
        return None

def save_checkpoint(last_modified):
    data = {"last_modified": last_modified.isoformat()}
    s3.put_object(
        Bucket=bucket,
        Key=checkpoint_key,
        Body=json.dumps(data)
    )

def process():
    checkpoint = load_checkpoint()
    print("🔍 Checkpoint atual:", checkpoint)

    response = s3.list_objects_v2(Bucket=bucket, Prefix=landing_prefix)
    if "Contents" not in response:
        print("Nenhum arquivo encontrado na landing.")
        return

    new_files = []
    for obj in response["Contents"]:
        key = obj["Key"]
        last_modified = obj["LastModified"].replace(tzinfo=timezone.utc)
        if checkpoint is None or last_modified > checkpoint:
            new_files.append((key, last_modified))

    if not new_files:
        print("Nenhum novo arquivo para processar.")
        return

    latest_modification = max([mod for _, mod in new_files])
    print(f"{len(new_files)} arquivos novos encontrados.")

    log_rows = []

    for key, mod in new_files:
        try:
            obj = s3.get_object(Bucket=bucket, Key=key)
            content = obj["Body"].read().decode("utf-8").strip().split("\n")
            records = [json.loads(line) for line in content if line]

            df = pd.DataFrame(records)
            df["source"] = "bbc"
            df['processed_at_ts'] = mod
            df["timestamp"] = pd.to_datetime(df["pubDate"], format="%a, %d %b %Y %H:%M:%S %Z")
            df["date"] = df["timestamp"].dt.strftime("%Y-%m-%d")

            for date, group in df.groupby("date"):
                buffer = io.BytesIO()
                group.to_parquet(buffer, index=False)
                buffer.seek(0)

                target_key = f"{bronze_prefix}source=bbc/date={date}/{os.path.basename(key).replace('.json', '.parquet')}"
                s3.upload_fileobj(buffer, Bucket=bucket, Key=target_key)

                print(f"Arquivo processado para {target_key}")

                # Adiciona linha ao log
                log_rows.append({
                    "path": f"s3://{bucket}/{target_key}",
                    "source": "bbc",
                    "processed_at": datetime.utcnow().isoformat(),
                    "record_count": len(group),
                    "status": "success"
                })

        except Exception as e:
            print(f"Erro ao processar {key}: {e}")
            log_rows.append({
                "path": f"s3://{bucket}/{bronze_prefix}{key}",
                "source": "bbc",
                "processed_at": datetime.utcnow().isoformat(),
                "record_count": 0,
                "status": "error"
            })

    if log_rows:
        log_df = pd.DataFrame(log_rows)
        log_date = datetime.utcnow().strftime("%Y-%m-%d")
        log_buffer = io.BytesIO()
        log_df.to_parquet(log_buffer, index=False)
        log_buffer.seek(0)

        log_key = f"{log_prefix}date={log_date}/bronze_log_{datetime.utcnow().strftime('%H%M%S')}.parquet"
        s3.upload_fileobj(log_buffer, Bucket=bucket, Key=log_key)
        print(f"Log de ingestão salvo em {log_key}")

    save_checkpoint(latest_modification)
    print("Novo checkpoint salvo:", latest_modification.isoformat())

if __name__ == "__main__":
    process()

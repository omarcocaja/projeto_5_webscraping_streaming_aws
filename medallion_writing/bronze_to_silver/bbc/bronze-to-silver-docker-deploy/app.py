import boto3
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import json
import trafilatura
import openai
import os
from datetime import datetime
import io

# Configurações
bucket = "portfolio-projeto-cinco"
log_prefix = "datalake/logs/bronze_ingest_log/"
bronze_prefix = "datalake/bronze/"
silver_prefix = "datalake/silver/"
checkpoint_path = "datalake-checkpoints/bbc_silver.json"

s3 = boto3.client("s3")

def get_last_silver_checkpoint():
    try:
        obj = s3.get_object(Bucket=bucket, Key=checkpoint_path)
        data = json.loads(obj['Body'].read().decode("utf-8"))
        return datetime.fromisoformat(data["last_processed_at"])
    except:
        return datetime.min

def update_silver_checkpoint():
    novo_checkpoint = datetime.now().isoformat()
    s3.put_object(
        Bucket=bucket,
        Key=checkpoint_path,
        Body=json.dumps({"last_processed_at": novo_checkpoint})
    )

def listar_arquivos_bronze():
    last_checkpoint = get_last_silver_checkpoint()
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=log_prefix)

    arquivos_validos = []
    for page in pages:
        for obj in page.get("Contents", []):
            if not obj["Key"].endswith(".parquet"):
                continue
            buffer = io.BytesIO()
            s3.download_fileobj(bucket, obj["Key"], buffer)
            buffer.seek(0)
            table = pq.read_table(buffer)
            df = table.to_pandas()

            df_filtrado = df[
                (df["source"] == "bbc") &
                (df["status"] == "success") &
                (pd.to_datetime(df["processed_at"]) > last_checkpoint)
            ]
            arquivos_validos.extend(df_filtrado["path"].tolist())

    return arquivos_validos

def carregar_dados_bronze():
    arquivos = listar_arquivos_bronze()
    if not arquivos:
        print("Nenhum novo arquivo para processar.")
        return pd.DataFrame()

    dfs = []
    for path in arquivos:
        buffer = io.BytesIO()
        s3.download_fileobj(bucket, path.replace(f"s3://{bucket}/", ""), buffer)
        buffer.seek(0)
        table = pq.read_table(buffer)
        dfs.append(table.to_pandas())

    return pd.concat(dfs, ignore_index=True)

prompt_template = """
A partir do texto abaixo, extraia as seguintes informações:
- Um resumo em uma frase.
- O sentimento principal entre: POSITIVO, NEGATIVO ou NEUTRO.
- Lista de locais mencionados, incluindo países, estados e cidades.
- Lista de pessoas mencionadas.

Texto:
\"\"\"{texto}\"\"\"

Retorne no seguinte formato JSON:
{{
  "resumo": "...",
  "sentimento": "...",
  "locais": [...],
  "pessoas": [...]
}}
"""

def extrair_texto(link):
    try:
        baixado = trafilatura.fetch_url(link)
        if baixado:
            return trafilatura.extract(baixado, include_comments=False, include_tables=False)
        return ""
    except Exception as e:
        print(f"Erro ao extrair texto de {link}: {e}")
        return ""

def enriquecer_artigo(texto):
    prompt = prompt_template.format(texto=texto)
    client = openai.OpenAI(api_key='')

    try:
        response = client.chat.completions.create(
            model="gpt-4.1-nano",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.3,
            max_tokens=300
        )
        result = (
            response.choices[0].message.content
            .replace("```", "")
            .replace("json", "")
        )
        return json.loads(result)
    except Exception as e:
        print(f"Erro ao enriquecer artigo: {e}")
        return {
            "resumo": None,
            "sentimento": None,
            "locais": [],
            "pessoas": []
        }

def lambda_handler(event, context):
    df = carregar_dados_bronze()
    if df.empty:
        print("Nenhum dado carregado.")
        return {"statusCode": 204, "body": "Sem novos dados."}

    df["article_text"] = df["link"].apply(extrair_texto)
    df["genai_output"] = df["article_text"].apply(enriquecer_artigo)
    df_exploded = pd.json_normalize(df["genai_output"])
    df_final = pd.concat([df.drop(columns=["genai_output"]), df_exploded], axis=1)

    df_final["processed_at"] = datetime.now()
    df_final["date"] = df_final["processed_at"].dt.strftime("%Y-%m-%d")
    df_final["source"] = "bbc"

    for date, group in df_final.groupby("date"):
        table = pa.Table.from_pandas(group)
        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        buffer.seek(0)

        filename = f"bbc_silver_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
        target_key = f"{silver_prefix}source=bbc/date={date}/{filename}"

        s3.upload_fileobj(buffer, Bucket=bucket, Key=target_key)
        print(f"Arquivo escrito em: s3://{bucket}/{target_key}")

    update_silver_checkpoint()
    print("Checkpoint atualizado.")

    return {
        "statusCode": 200,
        "body": f"{len(df_final)} registros enriquecidos e salvos."
    }

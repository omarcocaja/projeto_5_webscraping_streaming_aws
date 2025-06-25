import boto3
import requests
import json
from datetime import datetime, date
import os
import xmltodict

s3 = boto3.client('s3')

RSS_URL = "https://feeds.bbci.co.uk/news/world/rss.xml"
CHECKPOINT_BUCKET = 'portfolio-projeto-cinco'
CHECKPOINT_KEY = "crawler-checkpoints/bbc_checkpoint.json"
TIMESTAMP = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
DATA_HOJE = date.isoformat(date.today())

def safe_strip(value):
    if isinstance(value, dict):
        return value.get("#text", "").strip()
    elif isinstance(value, str):
        return value.strip()
    return ""

def load_checkpoint():
    try:
        response = s3.get_object(Bucket=CHECKPOINT_BUCKET, Key=CHECKPOINT_KEY)
        content = response['Body'].read().decode('utf-8')
        data = json.loads(content)
        return data.get("last_pubDate").strip()
    except s3.exceptions.NoSuchKey:
        return None


def save_checkpoint(last_pubDate):
    s3.put_object(
        Bucket=CHECKPOINT_BUCKET,
        Key=CHECKPOINT_KEY,
        Body=json.dumps({"last_pubDate": last_pubDate})
    )


def lambda_handler(event, context):
    response = requests.get(RSS_URL)
    response.raise_for_status()
    dict_xml = xmltodict.parse(response.text)

    items = dict_xml["rss"]["channel"]["item"]
    if not isinstance(items, list):  # Quando só há 1 item no RSS
        items = [items]

    last_date_str = load_checkpoint()

    if last_date_str and "GMT" not in last_date_str:
        last_date_str += " GMT"
    
    last_date = datetime.strptime(last_date_str, "%a, %d %b %Y %H:%M:%S %Z") if last_date_str else None
    max_date = last_date
    new_items = []

    for item in items:
        pub_date_str = item["pubDate"].strip()
        pub_date = datetime.strptime(pub_date_str, "%a, %d %b %Y %H:%M:%S %Z")

        if last_date and pub_date <= last_date:
            continue

        news = {
            "title": safe_strip(item.get("title")),
            "description": safe_strip(item.get("description")),
            "link": safe_strip(item.get("link")),
            "pubDate": pub_date_str,
            "guid": safe_strip(item.get("guid")),
            "source": "bbc"
        }

        s3.put_object(
            Bucket=CHECKPOINT_BUCKET,
            Key=f"datalake/landing/bbc/bbc_{TIMESTAMP}_{news['title'].lower().replace(' ', '_')}.json",
            Body=json.dumps(news)
        )

        new_items.append(news)

        if not max_date or pub_date > max_date:
            max_date = pub_date

    if max_date:
        save_checkpoint(max_date.strftime("%a, %d %b %Y %H:%M:%S %Z"))

    print(f"{len(new_items)} notícias novas enviadas para a fila.")
    return {"count": len(new_items)}


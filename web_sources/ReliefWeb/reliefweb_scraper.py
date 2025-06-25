import requests
from bs4 import BeautifulSoup
import json
import time
from datetime import datetime
import sys
import os
from dotenv import load_dotenv
load_dotenv()

ROOT_DIR = os.environ['ROOT_DIR']
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from kafka_files.kafka_utils import write_to_kafka

BASE_URL = "https://reliefweb.int/updates?view=headlines"
CHECKPOINT_PATH = "web_sources/ReliefWeb/checkpoint_reliefweb.json"

def load_checkpoint():
    if not os.path.exists(CHECKPOINT_PATH):
        return None
    with open(CHECKPOINT_PATH, 'r') as f:
        return json.load(f).get("last_url")

def save_checkpoint(last_url):
    with open(CHECKPOINT_PATH, 'w') as f:
        json.dump({"last_url": last_url}, f)

def fetch_reliefweb_news(last_url):
    headers = {"User-Agent": "Mozilla/5.0"}
    response = requests.get(BASE_URL, headers=headers)
    response.raise_for_status()
    soup = BeautifulSoup(response.content, "html.parser")

    cards = soup.select("article.rw-river-article--card")
    new_data = []
    latest_url = None

    for card in cards:
        title_el = card.select_one("h3.rw-river-article__title a")
        if not title_el:
            continue

        title = title_el.text.strip()
        link = title_el["href"]
        full_link = link if link.startswith("http") else "https://reliefweb.int" + link

        if full_link == last_url:
            break

        summary_el = card.select_one("div.rw-river-article__content p")
        summary = summary_el.text.strip() if summary_el else ""

        country_el = card.select_one("p.rw-entity-country-slug a")
        country = country_el.text.strip() if country_el else ""

        source_el = card.select_one("dd.rw-entity-meta__tag-value--source")
        origin_source = source_el.text.strip() if source_el else ""

        time_tags = card.select("footer time")
        posted_at = time_tags[0]["datetime"] if len(time_tags) > 0 and time_tags[0].has_attr("datetime") else datetime.utcnow().isoformat()
        original_published = time_tags[1]["datetime"] if len(time_tags) > 1 and time_tags[1].has_attr("datetime") else ""

        news = {
            "title": title,
            "link": full_link,
            "post_id": full_link.split("/")[-1],
            "posted_at": posted_at,
            "original_published": original_published,
            "summary": summary,
            "country": country,
            "origin_source": origin_source,
            "source": "reliefweb"
        }

        print(f"Enviando notícia do ReliefWeb: {news['title']}")
        write_to_kafka(
            broker_url="localhost:9092",
            topic="reliefweb",
            record=news
        )

        new_data.append(news)
        latest_url = full_link if not latest_url else latest_url
        time.sleep(1)

    return new_data, latest_url

def main():
    last_url = load_checkpoint()
    data, latest_url = fetch_reliefweb_news(last_url)

    if data:
        save_checkpoint(latest_url)
        print(f"{len(data)} notícias enviadas com sucesso.")
    else:
        print("ℹNenhuma nova notícia encontrada.")

if __name__ == "__main__":
    main()

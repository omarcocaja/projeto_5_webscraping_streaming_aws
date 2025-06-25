import requests
from bs4 import BeautifulSoup
import yaml
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

CONFIG_PATH = "web_sources/Reddit/config.yaml"
CHECKPOINT_PATH = "web_sources/Reddit/checkpoint_reddit.json"

def load_config():
    with open(CONFIG_PATH, 'r') as f:
        config = yaml.safe_load(f)
    return config["subreddits"]

def load_checkpoint():
    if not os.path.exists(CHECKPOINT_PATH):
        return {}
    with open(CHECKPOINT_PATH, 'r') as f:
        return json.load(f)

def save_checkpoint(cp_dict):
    with open(CHECKPOINT_PATH, 'w') as f:
        json.dump(cp_dict, f)

def fetch_rss(subreddit, last_link):
    rss_url = f"https://www.reddit.com/r/{subreddit}/.rss"
    headers = {"User-Agent": "Mozilla/5.0"}
    response = requests.get(rss_url, headers=headers)
    response.raise_for_status()

    soup = BeautifulSoup(response.content, "xml")
    items = soup.find_all("entry")

    new_items = []
    latest_link = None

    for item in items:
        link = item.link["href"]
        if link == last_link:
            break

        post = {
            "title": item.title.text.strip(),
            "link": link,
            "post_id": link.split("/")[-2],
            "subreddit": subreddit,
            "posted_at": item.updated.text.strip(),
            "source": "reddit"
        }

        print(f"Enviando post do subreddit '{subreddit}': {post['title']}")
        write_to_kafka(
            broker_url="localhost:9092",
            topic="reddit",
            record=post
        )

        new_items.append(post)
        latest_link = link if not latest_link else latest_link
        time.sleep(1)

    return new_items, latest_link

def main():
    subreddits = load_config()
    checkpoint = load_checkpoint()
    new_checkpoint = {}

    for sub in subreddits:
        last_link = checkpoint.get(sub)
        data, latest_link = fetch_rss(sub, last_link)
        if data and latest_link:
            new_checkpoint[sub] = latest_link

    if new_checkpoint:
        save_checkpoint(new_checkpoint)
        print("Mensagens enviadas e checkpoint atualizado.")
    else:
        print("Nenhum novo post encontrado.")

if __name__ == "__main__":
    main()

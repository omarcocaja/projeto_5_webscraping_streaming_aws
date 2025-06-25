import os
import sys
import json
import time
from datetime import datetime
from dotenv import load_dotenv

try:
    from playwright.sync_api import sync_playwright
except:
    os.system("pip install playwright")
    os.system("playwright install")
    from playwright.sync_api import sync_playwright

load_dotenv()

ROOT_DIR = os.environ['ROOT_DIR']
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from kafka_files.kafka_utils import write_to_kafka

BASE_URL = "https://www.emsc-csem.org/Earthquake_information/"
CHECKPOINT_PATH = "web_sources/EMSC/checkpoint_emsc.json"
OUTPUT_DIR = '/home/omarcocaja/Desktop/portfolio/projeto_5_webscraping/web_sources/EMSC'

def load_checkpoint():
    """
    """
    if not os.path.exists(CHECKPOINT_PATH):
        return None
    with open(CHECKPOINT_PATH, "r") as f:
        return json.load(f).get("last_link")

def save_checkpoint(last_link):
    """
    """
    with open(CHECKPOINT_PATH, "w") as f:
        json.dump({"last_link": last_link}, f)

def fetch_emsc_events(playwright, last_link):
    """
    """
    browser = playwright.chromium.launch(headless=True)
    page = browser.new_page()
    page.goto(BASE_URL, timeout=60000)

    try:
        page.wait_for_selector("tr.lilist", timeout=15000)
    except Exception as e:
        print("Falha ao localizar tabela principal:", str(e))
        screenshot_path = os.path.join(OUTPUT_DIR, "debug_emsc.png")
        page.screenshot(path=screenshot_path)
        print(f"Screenshot salvo: {screenshot_path}")
        browser.close()
        return []

    rows = page.query_selector_all("tr.lilist")
    new_data = []
    latest_link = None

    for row in rows:
        # Campos por classe
        date_el = row.query_selector("td.tbdat a")
        datetime_str = date_el.inner_text().split("\n")[0].strip() if date_el else ""

        lat_el = row.query_selector("td.tblat")
        latitude = lat_el.inner_text().strip() if lat_el else ""

        lon_el = row.query_selector("td.tblon")
        longitude = lon_el.inner_text().strip() if lon_el else ""

        depth_el = row.query_selector("td.tbdep")
        depth = depth_el.inner_text().strip() if depth_el else ""

        mag_el = row.query_selector("td.tbmag")
        magnitude = mag_el.inner_text().strip() if mag_el else ""

        region_el = row.query_selector("td.tbreg")
        region = region_el.inner_text().strip() if region_el else ""

        link_el = row.query_selector("td.tbdat a")
        href = link_el.get_attribute("href") if link_el else ""
        link = "https://www.emsc-csem.org" + href if href else ""
        post_id = href.split("=")[-1] if "id=" in href else ""

        if link == last_link:
            break

        event = {
            "datetime": datetime_str,
            "latitude": latitude,
            "longitude": longitude,
            "depth_km": depth,
            "magnitude": magnitude,
            "region": region,
            "link": link,
            "post_id": post_id,
            "source": "emsc"
        }

        print(f"Enviando evento ao Kafka: {event['datetime']} - {event['region']}")
        write_to_kafka(
            broker_url="localhost:9092",
            topic="emsc",
            record=event
        )

        new_data.append(event)
        latest_link = link if not latest_link else latest_link

        time.sleep(1)  # evita flood no Kafka

    browser.close()
    return new_data, latest_link

def main():
    last_link = load_checkpoint()
    with sync_playwright() as p:
        data, latest_link = fetch_emsc_events(p, last_link)

    if data:
        save_checkpoint(latest_link)
        print(f"{len(data)} eventos enviados com sucesso.")
    else:
        print("Nenhum novo evento encontrado.")

if __name__ == "__main__":
    main()

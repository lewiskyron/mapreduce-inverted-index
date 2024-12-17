import requests
from bs4 import BeautifulSoup
import time
import json
import sys
import os
from flask import Flask, jsonify
import threading
import logging


WIKIPEDIA_BASE_URL = "https://en.wikipedia.org"
CATEGORY_URL = "https://en.wikipedia.org/wiki/Category:Rock_musical_group_stubs"
# CATEGORY_URL = "https://en.wikipedia.org/wiki/Category:Living_people"


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s:%(message)s",
    handlers=[logging.FileHandler("master.log"), logging.StreamHandler(sys.stdout)],
)


app = Flask(__name__)

is_scraping = False


def get_artist_page_urls(category_url):
    artist_urls = []
    while category_url:
        try:
            response = requests.get(category_url)
            if response.status_code != 200:
                print(f"Failed to retrieve {category_url}", file=sys.stderr)
                break
            soup = BeautifulSoup(response.text, "html.parser")

            # Find all links to artist pages
            for li in soup.find_all("li"):
                a_tag = li.find("a")
                if a_tag and "href" in a_tag.attrs:
                    href = a_tag["href"]
                    if href.startswith("/wiki/") and not ":" in href:
                        full_url = WIKIPEDIA_BASE_URL + href
                        artist_urls.append(full_url)

            # Check for next page
            next_page = soup.find("a", string="next page")
            if next_page and "href" in next_page.attrs:
                category_url = WIKIPEDIA_BASE_URL + next_page["href"]
                time.sleep(1)  # Be polite and avoid hammering Wikipedia
            else:
                category_url = None
        except Exception as e:
            logging.error(f"Error processing {category_url}: {e}")
            break
    return artist_urls


def distribute_to_mappers(artist_urls, mapper_urls, chunk_size=5):
    """
    Distributes URLs to mappers by sending HTTP POST requests to their /map endpoints.
    """
    mapped_terms_all = []
    doc_id_start = 0

    for i, mapper_url in enumerate(mapper_urls):
        # Determine the subset of URLs for this mapper
        start_index = i * chunk_size
        end_index = start_index + chunk_size
        subset = artist_urls[start_index:end_index]

        if not subset:
            continue

        payload = {"urls": subset, "doc_id_start": doc_id_start}

        try:
            response = requests.post(f"{mapper_url}/map", json=payload)
            if response.status_code == 200:
                data = response.json()
                mapped_terms = data.get("mapped_terms", [])
                mapped_terms_all.extend(mapped_terms)
                doc_id_start += len(subset)
                logging.info(
                    f"Mapper {i} processed {len(subset)} URLs and returned {len(mapped_terms)} mappings"
                )
            else:
                logging.error(
                    f"Mapper {i} failed with status code {response.status_code}"
                )
        except Exception as e:
            logging.error(f"Error communicating with mapper {i}: {e}", exc_info=True)

    logging.info(f"Total mapped terms collected: {len(mapped_terms_all)}")
    return mapped_terms_all

@app.route("/scrape_uris", methods=["POST"])
def scrape_uris():
    global is_scraping
    """
    Endpoint to trigger the URI scraping process.
    
    Returns:
        JSON response indicating the status of the scraping task.
    """
    # Define the path to the JSON file
    json_file_path = os.path.join(
        os.getcwd(), "artist_URIs.json"
    )  # Adjust the path if needed

    # Check if the JSON file already exists
    if os.path.exists(json_file_path):
        logging.info("artist_URI.json already exists. Skipping scraping.")
        logging.info("Distributing URIs to mappers for processing.")
        try:
            with open(json_file_path, 'r', encoding='utf-8') as f:
                artist_urls = json.load(f)
            
            if not isinstance(artist_urls, list):
                logging.error(f"Invalid JSON format in {json_file_path}. Expected a list of URLs.")
                artist_urls = []
            
            logging.info(f"Loaded {len(artist_urls)} URLs from {json_file_path}")
            
            mapper_urls = os.getenv("MAPPER_URLS", "").split(",")
            mapper_urls = [url.strip() for url in mapper_urls if url.strip()]
            
            if artist_urls and mapper_urls:
                mapped_terms = distribute_to_mappers(artist_urls, mapper_urls, chunk_size=10)
            else:
                logging.warning("No URLs or mapper URLs available for processing")

        except json.JSONDecodeError:
            logging.error(f"Error decoding JSON from {json_file_path}")
        except FileNotFoundError:
            logging.error(f"JSON file not found: {json_file_path}")
        except Exception as e:
            logging.error(f"Unexpected error processing URLs: {e}")

        return (
            jsonify(
                {
                    "status": "exists",
                    "message": "active_URI.json already exists. Scraping task skipped.",
                }
            ),
            200,
        )
    # Check if a scraping task is already running
    if is_scraping:
        return (
            jsonify(
                {"status": "error", "message": "Scraping task is already running."}
            ),
            429,
        )

    # Set the scraping flag to True
    is_scraping = True

    # Run the scraping in a separate thread to avoid blocking
    thread = threading.Thread(target=scrape_and_save)
    thread.start()
    logging.info("Scraping thread started.")
    return jsonify({"status": "started", "message": "URI scraping initiated."}), 202


def scrape_and_save():
    """
    Orchestrates the scraping process and saves the collected artist URLs to a JSON file.
    """
    logging.info("Starting URI scraping process...")
    artist_urls = get_artist_page_urls(CATEGORY_URL)
    logging.info(f"Collected {len(artist_urls)} artist URLs.")
    logging.info("Scraping Process Completed")

    # Serialize the data to JSON using print statements
    try:
        with open("artist_URIs.json", "w", encoding="utf-8") as f:
            json.dump(artist_urls, f, ensure_ascii=False, indent=4)
        print("Data successfully written to active_URI.json")

        # Distribute URLs to mappers
        mapper_urls = os.getenv("MAPPER_URLS", "").split(",")
        mapper_urls = [url.strip() for url in mapper_urls if url.strip()]
        mapped_terms = distribute_to_mappers(artist_urls, mapper_urls, chunk_size=10)

    except Exception as e:
        print(f"Failed to write JSON file: {e}", file=sys.stderr)

    finally:
        is_scraping = False


def main():
    app.run(host="0.0.0.0", port=5001)


if __name__ == "__main__":
    main()

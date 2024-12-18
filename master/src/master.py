from flask import Flask, jsonify, request
from .coordinator import MapReduceCoordinator
from .constants import (
    WIKIPEDIA_BASE_URL,
    CATEGORY_URL,
    MASTER_HOST,
    MASTER_PORT,
    LOG_FORMAT,
    LOG_LEVEL,
    MESSAGES,
    STATUS,
    ENV_MAPPER_URLS,
)
import requests
from bs4 import BeautifulSoup
import logging
import os
import time

# Configure logging
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format=LOG_FORMAT,
    handlers=[logging.StreamHandler()],
)


logger = logging.getLogger(__name__)
def initialize_coordinator():
    """Initialize and start the coordinator"""
    try:
        logger.info("Initializing MapReduceCoordinator...")
        coordinator = MapReduceCoordinator()
        logger.info("MapReduce Coordinator system initialized successfully")
        return coordinator
    except Exception as e:
        logger.error(f"Failed to initialize MapReduce system: {e}", exc_info=True)
        raise


app = Flask(__name__)
coordinator = initialize_coordinator()

# Track if scraping is active
is_scraping = False


def get_artist_page_urls(category_url):
    """Fetches artist page URLs from Wikipedia"""
    artist_urls = []
    current_url = category_url  # Keep original category_url unchanged

    while current_url:  # Continue until no next page is found
        try:
            response = requests.get(current_url)
            if response.status_code != 200:
                logger.error(f"Failed to retrieve {current_url}")
                break

            soup = BeautifulSoup(response.text, "html.parser")

            # Find all links to artist pages
            for li in soup.find_all("li"):
                a_tag = li.find("a")
                if a_tag and "href" in a_tag.attrs:
                    href = a_tag["href"]
                    if href.startswith("/wiki/") and ":" not in href:
                        full_url = WIKIPEDIA_BASE_URL + href
                        artist_urls.append(full_url)

            # Check for next page
            next_page = soup.find("a", string="next page")
            if next_page and "href" in next_page.attrs:
                current_url = WIKIPEDIA_BASE_URL + next_page["href"]
                logger.info(f"Found next page: {current_url}")
                time.sleep(1)  # Be polite and avoid hammering Wikipedia
            else:
                current_url = None
                logger.info("No more pages to process")

        except Exception as e:
            logger.error(f"Error processing {current_url}: {e}")
            break

    logger.info(f"Total URLs collected: {len(artist_urls)}")
    return artist_urls


@app.route("/register_mapper", methods=["POST"])
def register_mapper():
    """Endpoint for mappers to register themselves"""
    try:
        data = request.get_json()
        mapper_url = data.get("mapper_url")
        if not mapper_url:
            return jsonify({"error": "mapper_url is required"}), 400

        result = coordinator.register_mapper(mapper_url)
        return jsonify(result), 200

    except Exception as e:
        logger.error(f"Error registering mapper: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/scrape", methods=["POST"])
def start_scraping():
    """Endpoint to initiate URL scraping and distribution"""
    global is_scraping

    if is_scraping:
        return (
            jsonify(
                {"status": STATUS["ERROR"], "message": MESSAGES["SCRAPING_IN_PROGRESS"]}
            ),
            409,
        )

    is_scraping = True

    try:
        # Get mapper URLs from environment
        mapper_urls = os.getenv(ENV_MAPPER_URLS, "").split(",")
        mapper_urls = [url.strip() for url in mapper_urls if url.strip()]

        if not mapper_urls:
            raise ValueError(MESSAGES["NO_MAPPERS"])

        # Start the scraping process
        logger.info("Starting URL collection")
        urls = get_artist_page_urls(CATEGORY_URL)

        if not urls:
            raise ValueError(MESSAGES["NO_URLS"])

        # Use coordinator to distribute to mappers
        logger.info(f"Distributing {len(urls)} URLs to {len(mapper_urls)} mappers")
        results = coordinator.distribute_to_mappers(urls, mapper_urls)

        return (
            jsonify(
                {
                    "status": STATUS["SUCCESS"],
                    "message": f"Processing {len(urls)} URLs",
                    "distribution_results": results,
                }
            ),
            200,
        )

    except Exception as e:
        logger.error(f"Error in scraping process: {e}")
        return jsonify({"status": STATUS["ERROR"], "message": str(e)}), 500

    finally:
        is_scraping = False


if __name__ == "__main__":
    app.run(host=MASTER_HOST, port=MASTER_PORT)

import requests
from bs4 import BeautifulSoup
import re
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer
import nltk
import json
import sys
import logging
import os

# Configure logging for mapper_logic.py
logger = logging.getLogger(__name__)


# Initialize NLTK components
# Log NLTK data paths
STOP_WORDS = set()
def _nltk_diagnostic():
    # Diagnostic logging for NLTK data paths
    logger.debug(f"NLTK data paths: {nltk.data.path}")

    # Initialize NLTK components
    nltk_data_path = os.environ.get("NLTK_DATA", "/opt/nltk_data")
    logger.debug(f"Using NLTK data path: {nltk_data_path}")

    # Check if the directory exists and its contents
    if os.path.exists(nltk_data_path):
        logger.debug(f"NLTK data directory contents: {os.listdir(nltk_data_path)}")
    else:
        logger.warning(f"NLTK data directory does not exist: {nltk_data_path}")

    nltk.data.path.append(nltk_data_path)

    try:
        from nltk.corpus import stopwords
        global STOP_WORDS
        STOP_WORDS = set(stopwords.words("english"))
        logger.info(f"Successfully loaded {len(STOP_WORDS)} stop words")
        return STOP_WORDS
    except Exception as e:
        logger.error(f"Failed to load stopwords: {e}", exc_info=True)


def fetch_page_content(url):
    """
    Fetches and extracts text content from a Wikipedia page.
    """
    try:
        response = requests.get(url)
        if response.status_code != 200:
            logger.warning(
                "Failed to retrieve %s with status code %s", url, response.status_code
            )
            return ""
        soup = BeautifulSoup(response.text, "html.parser")

        # Extract main content
        content_div = soup.find("div", {"id": "mw-content-text"})
        if not content_div:
            logger.warning("No content div found for URL: %s", url)
            return ""

        # Remove unwanted elements
        for element in content_div(["table", "script", "style"]):
            element.decompose()

        text = content_div.get_text(separator=" ", strip=True)
        return text
    except Exception as e:
        logger.error("Error fetching %s: %s", url, e, exc_info=True)
        return ""


def preprocess_text(text):
    """
    Preprocesses the text by lowercasing, removing non-alphabetic characters,
    tokenizing, removing stop words, and stemming.
    """
    text = text.lower()
    text = re.sub(r"[^a-z\s]", "", text)
    tokens = text.split()

    filtered_tokens = [token for token in tokens if token not in STOP_WORDS]
    return filtered_tokens


def map_terms_to_documents(urls, doc_id_start=0):
    """
    Maps terms to document IDs for a list of URLs.
    """
    mapped = []
    doc_id = doc_id_start
    logging.info(f"{len(urls)}")
    for url in urls:
        logger.info("Processing URL: %s with doc_id: %s", url, doc_id)
        text = fetch_page_content(url)
        if not text:
            logger.info("No content fetched for URL: %s", url)
            doc_id += 1
            continue
        terms = preprocess_text(text)
        unique_terms = set(terms)
        for term in unique_terms:
            mapped.append(
                {
                    "term": term,
                    "doc_id": doc_id,
                    "url": url,  # Optional: include URL for reference
                }
            )
        doc_id += 1
    return mapped

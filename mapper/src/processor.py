import logging
from typing import List, Dict
import requests
from bs4 import BeautifulSoup
import re
from nltk.corpus import stopwords
import nltk
import os
import json
from .constants import INTERMEDIATE_DIR, INTERMEDIATE_FILE_FORMAT

logger = logging.getLogger(__name__)


class MapperProcessor:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.stop_words = set()
        self.initialize_nltk()

    def initialize_nltk(self):
        """Initialize NLTK components"""
        try:
            nltk_data_path = os.environ.get("NLTK_DATA", "/opt/nltk_data")
            nltk.data.path.append(nltk_data_path)
            self.stop_words = set(stopwords.words("english"))
            self.logger.info(f"Successfully loaded {len(self.stop_words)} stop words")
        except Exception as e:
            self.logger.error(f"Failed to load stopwords: {e}", exc_info=True)
            raise

    def save_intermediate_results(
        self, mapped_terms: List[Dict], mapper_id: int
    ) -> str:
        """
        Save mapped terms to local disk as intermediate results
        Returns the path to the saved file
        """
        try:
            # Ensure intermediate directory exists
            os.makedirs(INTERMEDIATE_DIR, exist_ok=True)

            # Create intermediate file path
            file_path = os.path.join(
                INTERMEDIATE_DIR,
                INTERMEDIATE_FILE_FORMAT.format(
                    mapper_id=mapper_id, part_id=0  # For now using single part
                ),
            )

            # Write results to file
            with open(file_path, "w") as f:
                json.dump(mapped_terms, f, indent=2)

            self.logger.info(f"Saved intermediate results to {file_path}")
            return file_path

        except Exception as e:
            self.logger.error(f"Error saving intermediate results: {e}")
            raise

    def fetch_page_content(self, url: str) -> str:
        """Fetches and extracts text content from a Wikipedia page"""
        try:
            response = requests.get(url)
            if response.status_code != 200:
                self.logger.warning(
                    f"Failed to retrieve {url} with status code {response.status_code}"
                )
                return ""

            soup = BeautifulSoup(response.text, "html.parser")
            content_div = soup.find("div", {"id": "mw-content-text"})

            if not content_div:
                self.logger.warning(f"No content div found for URL: {url}")
                return ""

            # Remove unwanted elements
            for element in content_div(["table", "script", "style"]):
                element.decompose()

            text = content_div.get_text(separator=" ", strip=True)
            return text

        except Exception as e:
            self.logger.error(f"Error fetching {url}: {e}", exc_info=True)
            return ""

    def preprocess_text(self, text: str) -> List[str]:
        """Preprocesses text by lowercasing, removing non-alphabetic characters,
        tokenizing, and removing stop words"""
        text = text.lower()
        text = re.sub(r"[^a-z\s]", "", text)
        tokens = text.split()

        self.logger.debug(f"Tokens before removing stop words: {len(tokens)}")
        filtered_tokens = [token for token in tokens if token not in self.stop_words]
        self.logger.debug(f"Tokens after removing stop words: {len(filtered_tokens)}")

        return filtered_tokens

    def map_terms_to_documents(
        self, urls: List[str], doc_id_start: int = 0
    ) -> List[Dict]:
        """Maps terms to document IDs for a list of URLs"""
        mapped = []
        doc_id = doc_id_start

        for url in urls:
            self.logger.info(f"Processing URL: {url} with doc_id: {doc_id}")
            text = self.fetch_page_content(url)

            if not text:
                self.logger.info(f"No content fetched for URL: {url}")
                doc_id += 1
                continue

            terms = self.preprocess_text(text)
            unique_terms = set(terms)
            logging.info(f"Terms for document {doc_id}: {unique_terms}")

            for term in unique_terms:
                mapped.append({"term": term, "doc_id": doc_id, "url": url})

            doc_id += 1
            self.logger.info(
                f"Processed document {doc_id-1}: found {len(unique_terms)} unique terms"
            )

        return mapped

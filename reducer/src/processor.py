import logging
import os
import json
from typing import Dict, List
from datetime import datetime
import time

logger = logging.getLogger(__name__)


class ReducerProcessor:
    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def process_intermediate_results(self, results: List[Dict]) -> Dict[str, List]:
        """Process intermediate results to create final inverted index"""
        inverted_index = {}

        # Log incoming data
        self.logger.info(f"Received {len(results)} results to process")

        # Check if results need to be parsed from JSON
        if len(results) > 0 and isinstance(results[0], str):
            self.logger.info("Results are strings, attempting to parse JSON")
            parsed_results = []
            for result in results:
                try:
                    parsed_result = json.loads(result)
                    parsed_results.append(parsed_result)
                except json.JSONDecodeError as e:
                    self.logger.error(f"Failed to parse result as JSON: {e}")
                    self.logger.error(f"Problematic data: {result[:200]}...")  # Log first 200 chars
            results = parsed_results

        # Process each result
        for i, result in enumerate(results):
            self.logger.info(f"Processing result {i+1}/{len(results)}")

            # Get terms from the result
            terms = result.get("terms", [])
            self.logger.info(f"Found {len(terms)} terms in result {i+1}")

            # Process each term
            for term_data in terms:
                try:
                    term = term_data["term"]
                    occurrences = term_data.get("occurrences", [])

                    if term not in inverted_index:
                        inverted_index[term] = []

                    inverted_index[term].extend(occurrences)
                    self.logger.debug(f"Processed term '{term}' with {len(occurrences)} occurrences")

                except KeyError as e:
                    self.logger.error(f"Missing key in term_data: {e}")
                    self.logger.error(f"Term data structure: {term_data}")
                    continue
                except Exception as e:
                    self.logger.error(f"Error processing term data: {e}")
                    self.logger.error(f"Problematic term data: {term_data}")
                    continue

        # Sort and deduplicate
        for term in inverted_index:
            # Sort by doc_id
            inverted_index[term].sort(key=lambda x: x["doc_id"])

            # Remove duplicates
            seen_docs = set()
            unique_occurrences = []
            for occ in inverted_index[term]:
                doc_id = occ["doc_id"]
                if doc_id not in seen_docs:
                    seen_docs.add(doc_id)
                    unique_occurrences.append(occ)
            inverted_index[term] = unique_occurrences

        # Log final results
        self.logger.info(f"Created inverted index with {len(inverted_index)} terms")
        if len(inverted_index) > 0:
            self.logger.info(f"Inverted Index created successfully!")
        else:
            self.logger.warning("Inverted index is empty!")
            self.logger.warning("Input data summary:")
            self.logger.warning(f"Results length: {len(results)}")

        return inverted_index

    def save_final_index(self, index: Dict, output_dir: str = "data") -> str:
        """Save the final inverted index to disk in the data directory."""
        # Ensure the data directory exists
        os.makedirs(output_dir, exist_ok=True)

        timestamp = int(time.time())
        output_file = os.path.join(output_dir, f"inverted_index_{timestamp}.json")

        with open(output_file, "w") as f:
            json.dump(
                {
                    "metadata": {
                        "creation_time": datetime.now().isoformat(),
                        "num_terms": len(index),
                        "timestamp": timestamp,
                    },
                    "index": index,
                },
                f,
                indent=2,
            )

        logging.info(f"Inverted index saved to: {os.path.abspath(output_file)}")
        return output_file

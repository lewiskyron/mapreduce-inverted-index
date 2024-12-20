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

        for result in results:
            for term_data in result.get("terms", []):
                term = term_data["term"]
                if term not in inverted_index:
                    inverted_index[term] = []

                # Add all occurrences for this term
                inverted_index[term].extend(term_data["occurrences"])

        # Sort document IDs for each term for consistency
        for term in inverted_index:
            inverted_index[term].sort(key=lambda x: x["doc_id"])

        return inverted_index

    def save_final_index(self, index: Dict, output_dir: str) -> str:
        """Save the final inverted index to disk"""
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

        return output_file

# This file defines the available map functions that mappers should implement
MAP_FUNCTIONS = {
    "process_wikipedia_urls": {
        "name": "process_wikipedia_urls",
        "description": "Processes Wikipedia URLs to extract terms",
        "expected_input": "list of URLs",
        "expected_output": "list of (term, doc_id) pairs",
    }
}

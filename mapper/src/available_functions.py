from .processor import MapperProcessor
from typing import Dict


class FunctionRegistry:
    def __init__(self):
        self.processor = MapperProcessor()
        self.available_functions = {
            "process_wikipedia_urls": {
                "function": self.processor.map_terms_to_documents,
                "description": "Processes Wikipedia URLs to extract terms and create term-document pairs",
            }
        }

    def get_function(self, function_name: str):
        """Get a function by name from the registry"""
        if function_name not in self.available_functions:
            raise ValueError(f"Unknown function: {function_name}")
        return self.available_functions[function_name]["function"]

    def list_available_functions(self) -> Dict:
        """Return information about all available functions"""
        return {
            name: info["description"] for name, info in self.available_functions.items()
        }

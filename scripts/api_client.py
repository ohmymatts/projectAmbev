import requests
from typing import List, Dict
from datetime import datetime
import logging

class BreweryApiClient:
    BASE_URL = "https://api.openbrewerydb.org/v1/breweries"
    
    def __init__(self, max_retries=3, timeout=30):
        self.max_retries = max_retries
        self.timeout = timeout
        self.logger = logging.getLogger(__name__)
        
    def get_breweries(self, page: int = 1, per_page: int = 50) -> List[Dict]:
        """Fetch breweries from the API with pagination"""
        params = {"page": page, "per_page": per_page}
        
        for attempt in range(self.max_retries):
            try:
                response = requests.get(
                    self.BASE_URL,
                    params=params,
                    timeout=self.timeout
                )
                response.raise_for_status()
                return response.json()
                
            except requests.exceptions.RequestException as e:
                self.logger.warning(f"Attempt {attempt + 1} failed: {str(e)}")
                if attempt == self.max_retries - 1:
                    raise
                
    def get_all_breweries(self) -> List[Dict]:
        """Fetch all breweries by paginating through results"""
        all_breweries = []
        page = 1
        per_page = 200  # Max allowed by API
        
        while True:
            breweries = self.get_breweries(page=page, per_page=per_page)
            if not breweries:
                break
            all_breweries.extend(breweries)
            page += 1
            
        return all_breweries
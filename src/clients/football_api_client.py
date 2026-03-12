from typing import Any, Dict, List

from src.utils.request_endpoint import RequestEndpoint

class FootballApiClient:
    def __init__(self, base_url: str, api_key: str) -> None:
        self.api_client = RequestEndpoint(base_url=base_url)

        self.headers = {"x-apisports-key": api_key}

    def get_countries(self) -> List[Dict[str, Any]]:
        response = self.api_client.get("/countries", headers=self.headers)

        data = response.json()

        return data.get("response", [])
    
    def get_leagues(self) -> List[Dict[str, Any]]:
        response = self.api_client.get("/leagues", headers=self.headers)

        data = response.json()

        return data.get("response", [])
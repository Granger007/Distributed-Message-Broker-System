import os
import requests
from typing import Optional, List

BROKER_URLS = os.getenv("BROKER_URLS", "http://127.0.0.1:8001,http://127.0.0.1:8002").split(",")

class LeaderDiscoverer:
    def __init__(self, broker_urls: List[str] = None):
        self.broker_urls = broker_urls if broker_urls is not None else BROKER_URLS
        self.cached_leader: Optional[str] = None
        self.session = requests.Session()

    def discover_leader(self) -> str:
        """Find the leader by asking any known broker."""
        for url in self.broker_urls:
            try:
                resp = self.session.get(f"{url}/metadata/leader", timeout=1.0)
                if resp.status_code == 200:
                    leader_url = resp.json().get("leader_url")
                    if leader_url:
                        self.cached_leader = leader_url
                        return leader_url
            except requests.RequestException:
                continue
        
        raise RuntimeError("Could not discover leader from any known brokers")

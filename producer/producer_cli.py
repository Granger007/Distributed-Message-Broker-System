import time
import argparse
import sys
import requests
from typing import Optional

from common.discovery import LeaderDiscoverer, BROKER_URLS

class Producer:
    def __init__(self, broker_urls: list[str]):
        self.discoverer = LeaderDiscoverer(broker_urls)
        self.session = self.discoverer.session

    def produce(self, value: str, key: Optional[str] = None) -> dict:
        """Produce a message, handling redirects and failures."""
        if not self.discoverer.cached_leader:
            self.discoverer.discover_leader()

        retries = 5
        backoff = 0.5

        for attempt in range(retries):
            try:
                payload = {"value": value}
                if key is not None:
                    payload["key"] = key
                
                resp = self.session.post(
                    f"{self.discoverer.cached_leader}/produce",
                    json=payload,
                    timeout=2.0
                )
                
                if resp.status_code == 200:
                    return resp.json()
                elif resp.status_code == 409:
                    # Not leader
                    error_data = resp.json()
                    new_leader = error_data.get("leader_url")
                    print(f"[Producer] Got 409 NOT_LEADER. Suggested leader: {new_leader}. Rediscovering...")
                    if new_leader:
                        self.discoverer.cached_leader = new_leader
                    else:
                        self.discoverer.discover_leader()
                elif resp.status_code == 503:
                    print("[Producer] Got 503 REPLICATION_FAILED. Leader might have died. Rediscovering...")
                    self.discoverer.discover_leader()
                else:
                    print(f"[Producer] Unexpected status code {resp.status_code}: {resp.text}")
                    resp.raise_for_status()

            except requests.RequestException as e:
                print(f"[Producer] Connection error: {e}. Rediscovering leader...")
                self.discoverer.discover_leader()
            
            time.sleep(backoff)
            backoff *= 2
        
        raise RuntimeError(f"Failed to produce message after {retries} retries")

def main():
    parser = argparse.ArgumentParser(description="Yak Producer CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Send command
    send_parser = subparsers.add_parser("send", help="Send a single message")
    send_parser.add_argument("value", type=str, help="Message value")
    send_parser.add_argument("--key", type=str, help="Optional message key")

    # Batch command
    batch_parser = subparsers.add_parser("batch", help="Send multiple messages")
    batch_parser.add_argument("--count", type=int, default=10, help="Number of messages to send")
    batch_parser.add_argument("--delay", type=float, default=0.1, help="Delay between messages in seconds")
    batch_parser.add_argument("--prefix", type=str, default="msg", help="Prefix for message values")

    args = parser.parse_args()

    producer = Producer(BROKER_URLS)

    try:
        if args.command == "send":
            print(f"Sending message: value='{args.value}', key={args.key}")
            resp = producer.produce(args.value, args.key)
            print(f"Success: {resp}")
        elif args.command == "batch":
            print(f"Sending {args.count} messages...")
            for i in range(args.count):
                val = f"{args.prefix}-{i}"
                print(f"Sending [{i+1}/{args.count}]: {val}")
                resp = producer.produce(val)
                print(f"  -> {resp}")
                if args.delay > 0:
                    time.sleep(args.delay)
            print("Batch finished.")
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()

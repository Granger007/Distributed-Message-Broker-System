import os
import time
import argparse
import sys
import requests

from common.discovery import LeaderDiscoverer, BROKER_URLS

OFFSET_FILE = "./data/consumer_offset.txt"

class Consumer:
    def __init__(self, broker_urls: list[str]):
        self.discoverer = LeaderDiscoverer(broker_urls)
        self.session = self.discoverer.session
        self.local_offset = self._load_offset()

    def _load_offset(self) -> int:
        if os.path.exists(OFFSET_FILE):
            try:
                with open(OFFSET_FILE, "r") as f:
                    return int(f.read().strip())
            except ValueError:
                return 0
        return 0

    def _save_offset(self, offset: int):
        os.makedirs(os.path.dirname(OFFSET_FILE), exist_ok=True)
        with open(OFFSET_FILE, "w") as f:
            f.write(str(offset))
        self.local_offset = offset

    def reset_offset(self):
        self._save_offset(0)
        print("[Consumer] Offset reset to 0.")

    def poll_once(self) -> int:
        """Poll for messages once, return number of messages consumed."""
        if not self.discoverer.cached_leader:
            self.discoverer.discover_leader()

        try:
            resp = self.session.get(
                f"{self.discoverer.cached_leader}/consume",
                params={"from_offset": self.local_offset, "max": 50},
                timeout=2.0
            )
            
            if resp.status_code == 200:
                data = resp.json()
                messages = data.get("messages", [])
                
                if not messages:
                    return 0

                for msg in messages:
                    print(f"Consumed [Offset: {msg['offset']}] Key: {msg.get('key')} | Value: {msg['value']}")
                
                next_offset = messages[-1]["offset"] + 1
                self._save_offset(next_offset)
                return len(messages)

            elif resp.status_code == 409:
                print(f"[Consumer] Got 409. Rediscovering...")
                self.discoverer.discover_leader()
                return 0
            else:
                print(f"[Consumer] Unexpected status code {resp.status_code}: {resp.text}")
                resp.raise_for_status()

        except requests.RequestException as e:
            print(f"[Consumer] Connection error: {e}. Rediscovering leader...")
            self.discoverer.discover_leader()
            return 0
        
        return 0

def main():
    parser = argparse.ArgumentParser(description="Yak Consumer CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Follow command
    follow_parser = subparsers.add_parser("follow", help="Continuously poll for new messages")
    follow_parser.add_argument("--interval", type=float, default=1.0, help="Polling interval in seconds")

    # Reset command
    subparsers.add_parser("reset", help="Reset the local consumer offset to 0")

    args = parser.parse_args()

    consumer = Consumer(BROKER_URLS)

    try:
        if args.command == "follow":
            print(f"[Consumer] Starting follow loop. Current offset: {consumer.local_offset}")
            while True:
                consumed = consumer.poll_once()
                if consumed == 0:
                    time.sleep(args.interval)
        elif args.command == "reset":
            consumer.reset_offset()
    except KeyboardInterrupt:
        print("\n[Consumer] Exiting follow loop.")
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()

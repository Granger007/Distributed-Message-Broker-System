import os
import json
import sys

def verify():
    # After failover from broker-1, broker-2 is the final leader
    log_file = "./data/broker-2/log.jsonl"
    
    if not os.path.exists(log_file):
        print(f"FAIL: Log file {log_file} does not exist.")
        sys.exit(1)

    print(f"Reading log file: {log_file}")
    messages = []
    
    with open(log_file, "r") as f:
        for line in f:
            if line.strip():
                try:
                    entry = json.loads(line)
                    messages.append(entry)
                except json.JSONDecodeError:
                    print(f"FAIL: Malformed JSON found in log: {line.strip()}")
                    sys.exit(1)
    
    # We expect 40 messages, msg-0 through msg-39
    expected_count = 40
    
    if len(messages) != expected_count:
        print(f"FAIL: Expected {expected_count} messages, but found {len(messages)}.")
        sys.exit(1)
    
    for i, entry in enumerate(messages):
        expected_val = f"msg-{i}"
        
        # Check offset is sequential
        if entry["offset"] != i:
            print(f"FAIL: Offset mismatch at index {i}. Expected offset {i}, got {entry['offset']}")
            sys.exit(1)
            
        # Check message content
        if entry["value"] != expected_val:
            print(f"FAIL: Value mismatch at index {i}. Expected '{expected_val}', got '{entry['value']}'")
            sys.exit(1)

    print("=========================================")
    print(f"PASS: Verified exactly {expected_count} messages present in order, with zero data loss.")
    print("=========================================")

if __name__ == "__main__":
    verify()

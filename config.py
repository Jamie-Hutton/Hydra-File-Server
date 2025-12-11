import json
import os
import hashlib

# --- CONFIGURATION FILE SETUP ---
HOST_CONFIG_FILE = "host_config.json"

def load_host_ip():
    """
    Loads the Yggdrasil host IP from the local, untracked configuration file.
    """
    if not os.path.exists(HOST_CONFIG_FILE):
        raise FileNotFoundError(
            f"FATAL: Host IP configuration file not found: {HOST_CONFIG_FILE}. "
            "Please create this file and add it to .gitignore."
        )
    
    try:
        with open(HOST_CONFIG_FILE, 'r') as f:
            config = json.load(f)
            host_ip = config.get("YGGDRASIL_IP")
            
            if not host_ip:
                raise ValueError(f"FATAL: '{HOST_CONFIG_FILE}' is missing the 'YGGDRASIL_IP' key.")

            # Simple validation for Yggdrasil prefix
            if not (host_ip.startswith("200:") or host_ip.startswith("2000:")):
                print("Warning: HOST_YGGDRASIL_IP does not start with the standard '200:' or '2000:' prefix.")
            
            return host_ip

    except json.JSONDecodeError:
        raise ValueError(f"FATAL: Failed to parse '{HOST_CONFIG_FILE}'. Check JSON formatting.")
    except Exception as e:
        raise Exception(f"FATAL: Error loading host configuration: {e}")

# --- NETWORK CONFIGURATION ---
HOST_YGGDRASIL_IP = load_host_ip()
PORT = 8888
CHUNK_SIZE = 1024 * 1024  # 1 MB per chunk

# --- DIRECTORY CONFIGURATION ---
FILE_DIRECTORY = "./shared_files"
DOWNLOAD_DIRECTORY = "./downloaded_files"
PEER_LIST_FILE = "known_peers.json"
# The Index that tracks all chunks this peer is hosting (used by REPORT_AVAILABILITY)
LOCAL_MASTER_INDEX_FILE = "local_index.json"

# --- UTILITY FUNCTIONS ---

def load_peers():
    """Loads the list of known Yggdrasil addresses."""
    if not os.path.exists(PEER_LIST_FILE):
        return []
    try:
        with open(PEER_LIST_FILE, 'r') as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        print(f"Warning: Failed to load {PEER_LIST_FILE}. Starting fresh.")
        return []

def save_peers(peers):
    """Saves the current list of known Yggdrasil addresses."""
    os.makedirs(os.path.dirname(PEER_LIST_FILE) or '.', exist_ok=True)
    with open(PEER_LIST_FILE, 'w') as f:
        unique_peers = list(set(peers))
        # Ensure the current host IP is always in the list
        if HOST_YGGDRASIL_IP not in unique_peers:
            unique_peers.append(HOST_YGGDRASIL_IP)

        json.dump(unique_peers, f, indent=4)

def get_local_hosted_hashes():
    """
    Reads the LOCAL_MASTER_INDEX_FILE and returns a list of
    ALL chunk hashes contained within it.
    """
    if not os.path.exists(LOCAL_MASTER_INDEX_FILE):
        return []

    hosted_hashes = []
    try:
        with open(LOCAL_MASTER_INDEX_FILE, 'r') as f:
            master_index = json.load(f)

        # Iterate through every file in the index
        for file_metadata in master_index.values():
            # Iterate through every chunk in the file
            for chunk in file_metadata.get('chunks', []):
                hosted_hashes.append(chunk['hash'])

    except Exception as e:
        print(f"Error reading local index: {e}")
        return []

    return hosted_hashes


def calculate_hash(filepath):
    """Calculates the SHA-256 hash of a file in chunks."""
    sha256 = hashlib.sha256()
    with open(filepath, 'rb') as f:
        while True:
            data = f.read(4096)
            if not data:
                break
            sha256.update(data)
    return sha256.hexdigest()

def ensure_initial_setup():
    """Ensures directories exist and creates a dummy file for testing."""
    os.makedirs(FILE_DIRECTORY, exist_ok=True)
    os.makedirs(DOWNLOAD_DIRECTORY, exist_ok=True)

    test_file_path = os.path.join(FILE_DIRECTORY, "test_doc.txt")
    if not os.path.exists(test_file_path):
        # Create a small test file
        with open(test_file_path, "w") as f:
            f.write("This is a test file for the Yggdrasil File Server.\n")
            f.write("It helps verify chunking and indexing functionality.")

        # Calculate size for the placeholder index
        test_file_size = os.path.getsize(test_file_path) # <-- Get actual size

        # Ensure 'total_size' is included in the placeholder index
        with open(LOCAL_MASTER_INDEX_FILE, "w") as f:
            json.dump({
                "test_doc.txt": {
                    "total_size": test_file_size, # <-- FIXED
                    "replication_factor": 2,
                    "chunks": [{"hash": "hash_A_chunk0", "size": 1024}]
                }
            }, f, indent=4)

    print(f"Initial setup complete. Test file created at {test_file_path}")

if __name__ == '__main__':
    # Initial setup for first run
    ensure_initial_setup()
    # Initialize or update peer list with self
    save_peers(load_peers())
    print(f"Host IP: {HOST_YGGDRASIL_IP}")


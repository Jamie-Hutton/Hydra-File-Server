import socket
import threading
import time
import json
import os
import sys

# --- IMPORTS FROM CONFIG.PY (Re-enabled) ---
# NOTE: Assuming config.py is available and contains all constants (HOST_YGGDRASIL_IP, PORT, FILE_DIRECTORY, etc.)
try:
    from config import *
except ImportError:
    print("Error: config.py not found. Please create it first.")
    sys.exit(1)

# --- IMPORTS FROM INDEXER.PY ---
# Using 'from indexer import *' means functions are called without the 'indexer.' prefix.
try:
    from indexer import index_file # Only explicitly import index_file
except ImportError:
    print("Error: indexer.py not found. Please ensure it is in the same directory.")
    sys.exit(1)

# --- AUTO-INDEXER FUNCTION ---

def auto_index_shared_files():
    """
    Scans the FILE_DIRECTORY and indexes any file found.
    Refreshes the local_index.json automatically.
    """
    print(f"\n[AUTO-INDEXER] Scanning '{FILE_DIRECTORY}' for files...")
    
    # 1. Load existing index to avoid re-hashing unchanged files (Optimization)
    if os.path.exists(LOCAL_MASTER_INDEX_FILE):
        try:
            with open(LOCAL_MASTER_INDEX_FILE, 'r') as f:
                current_index = json.load(f)
        except json.JSONDecodeError:
            current_index = {}
    else:
        current_index = {}

    files_indexed_count = 0
    
    # 2. Iterate over actual files in the directory
    for filename in os.listdir(FILE_DIRECTORY):
        filepath = os.path.join(FILE_DIRECTORY, filename)
        
        # Skip directories and hidden files
        if not os.path.isfile(filepath) or filename.startswith('.'):
            continue
            
        # Check if file needs indexing (Logic: If not in index OR size changed)
        file_needs_indexing = False
        current_size = os.path.getsize(filepath)
        
        if filename not in current_index:
            file_needs_indexing = True
            print(f"   ➕ New file found: {filename}")
        elif current_index[filename].get('total_size') != current_size:
            file_needs_indexing = True
            print(f"   🔄 File changed (re-indexing): {filename}")

        # 3. Run the Indexer
        if file_needs_indexing:
            # CALLING INDEXER FUNCTION DIRECTLY (index_file)
            filename_result, file_entry = index_file(filepath)
            
            if file_entry:
                current_index[filename] = file_entry
                files_indexed_count += 1

    # 4. Save the updated index back to disk
    if files_indexed_count > 0:
        with open(LOCAL_MASTER_INDEX_FILE, 'w') as f:
            json.dump(current_index, f, indent=4)
        print(f"   ✅ Auto-Indexing complete. Updated {files_indexed_count} files in index.")
    else:
        print("   ✅ Index is up to date.")

# --- CORE SERVER LOGIC (Rest of the functions) ---

def send_file_chunk(client_socket, filename, chunk_id, chunk_hash, chunk_size):
    """Handles the transfer of a specific chunk."""
    full_path = os.path.join(FILE_DIRECTORY, filename)

    if not os.path.exists(full_path):
        client_socket.sendall(b"ERROR: FILE_NOT_FOUND")
        return

    try:
        # Send the CHUNK_READY header
        header = f"CHUNK_READY:{chunk_id}:{chunk_size}:{chunk_hash}"
        client_socket.sendall(header.encode('utf-8'))
        client_socket.recv(1024) # Wait for client READY

        # Find the starting byte position of the chunk
        # NOTE: This logic assumes chunk_id * CHUNK_SIZE is the offset, 
        # which is redundant but harmless since the index now provides 'offset'.
        start_byte = chunk_id * CHUNK_SIZE 

        with open(full_path, 'rb') as f:
            f.seek(start_byte)
            chunk_data = f.read(chunk_size)
            client_socket.sendall(chunk_data)

        print(f"   ✅ Thread {threading.get_ident()}: Sent Chunk {chunk_id} of '{filename}'.")

    except Exception as e:
        print(f"   ❌ Thread {threading.get_ident()}: Chunk transfer error: {e}")


def handle_peer_connection(client_socket, peer_ip):
    """Runs in a new thread for every incoming connection."""
    thread_id = threading.get_ident()
    
    try:
        data = client_socket.recv(1024).decode('utf-8').strip()
        
        if data == "REQUEST_FILE_LIST":
            print(f"   ➡️ Thread {thread_id}: Request File List")
            
            # LOAD REAL INDEX
            if os.path.exists(LOCAL_MASTER_INDEX_FILE):
                with open(LOCAL_MASTER_INDEX_FILE, 'r') as f:
                    # Read the JSON as a string directly
                    file_index_json = f.read() 
            else:
                file_index_json = "{}"

            header = f"LIST_SIZE:{len(file_index_json)}"
            client_socket.sendall(header.encode('utf-8'))
            client_socket.recv(1024) 
            client_socket.sendall(file_index_json.encode('utf-8'))


        elif data.startswith("REQUEST_CHUNK:"):
            # Command format: REQUEST_CHUNK:filename:chunk_id
            try:
                parts = data.split(":")
                requested_filename = parts[1]
                requested_chunk_id = int(parts[2])
                
                print(f"   ➡️ Thread {thread_id}: Request for {requested_filename} (Chunk {requested_chunk_id})")

                # 1. Load the Master Index to find where this chunk lives
                if not os.path.exists(LOCAL_MASTER_INDEX_FILE):
                    client_socket.sendall(b"ERROR: INDEX_NOT_FOUND")
                    return

                with open(LOCAL_MASTER_INDEX_FILE, 'r') as f:
                    master_index = json.load(f)

                # 2. Lookup File and Chunk Metadata
                if requested_filename in master_index:
                    file_meta = master_index[requested_filename]
                    chunks = file_meta.get('chunks', [])
                    
                    # Find the specific chunk by ID
                    target_chunk = next((c for c in chunks if c['id'] == requested_chunk_id), None)
                    
                    if target_chunk:
                        # 3. We found the chunk metadata, now read the actual bytes
                        real_file_path = os.path.join(FILE_DIRECTORY, requested_filename)
                        
                        if os.path.exists(real_file_path):
                            with open(real_file_path, 'rb') as f:
                                f.seek(target_chunk['offset']) # Use the offset from the index
                                chunk_data = f.read(target_chunk['size'])
                                
                                # 4. Send the CHUNK_READY header with REAL metadata
                                header = f"CHUNK_READY:{target_chunk['id']}:{target_chunk['size']}:{target_chunk['hash']}"
                                client_socket.sendall(header.encode('utf-8'))
                                client_socket.recv(1024) # Wait for READY
                                client_socket.sendall(chunk_data)
                                print(f"   ✅ Sent Chunk {requested_chunk_id}")
                        else:
                            print(f"   ❌ File exists in index but NOT on disk: {real_file_path}")
                            client_socket.sendall(b"ERROR: FILE_MISSING_ON_DISK")
                    else:
                        client_socket.sendall(b"ERROR: CHUNK_ID_NOT_FOUND")
                else:
                    client_socket.sendall(b"ERROR: FILENAME_NOT_IN_INDEX")

            except Exception as e:
                print(f"Error handling chunk request: {e}")
                client_socket.sendall(b"ERROR: INTERNAL_SERVER_ERROR")
            
        elif data == "REQUEST_PEER_LIST":
            # Gossip Protocol for discovery
            current_peers = load_peers()
            if peer_ip not in current_peers:
                current_peers.append(peer_ip)
                save_peers(current_peers)
                print(f"   ➕ Learned new peer: {peer_ip}")

            peer_list_json = json.dumps(current_peers)
            header = f"PEER_LIST_SIZE:{len(peer_list_json)}"
            client_socket.sendall(header.encode('utf-8'))
            client_socket.recv(1024) 
            client_socket.sendall(peer_list_json.encode('utf-8'))
            
        elif data == "REPORT_AVAILABILITY":
            # Audit Protocol for self-healing
            hosted_hashes = get_local_hosted_hashes() 
            hashes_json = json.dumps(hosted_hashes)
            header = f"HASH_LIST_SIZE:{len(hashes_json)}"
            client_socket.sendall(header.encode('utf-8'))
            client_socket.recv(1024)
            client_socket.sendall(hashes_json.encode('utf-8'))
            
        else:
            client_socket.sendall(b"ERROR: Unknown command.")
            
    except Exception as e:
        print(f"❌ Thread {thread_id}: Connection error with {peer_ip}: {e}")
        
    finally:
        client_socket.close()


# --- GOSSIPER THREAD (Simplified Audit/Repair) ---

GOSSIP_INTERVAL_SECONDS = 180  # 3 minutes

def exchange_peers(target_ip, port):
    """Client function used by the Gossiper to exchange peer lists."""
    client_socket = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
    try:
        client_socket.connect((target_ip, port))
        client_socket.sendall("REQUEST_PEER_LIST".encode('utf-8'))
        
        # Omitted: Full header parsing and list merging for brevity
        client_socket.close()
        # print(f"   [Gossip]: Synced with {target_ip[:10]}")
        
    except Exception as e:
        # print(f"   [Gossip]: Failed to sync with {target_ip[:10]}: {e}")
        pass # Expected if peer is offline

class PeerGossiper(threading.Thread):
    def __init__(self, port):
        super().__init__()
        self.port = port
        self.stop_event = threading.Event()

    def run(self):
        while not self.stop_event.is_set():
            print("\n[GOSSIPER] 🔄 Starting scheduled peer list exchange...")
            current_peers = load_peers()
            
            # 1. Exchange lists to discover new peers
            for target_ip in list(current_peers):
                if target_ip != HOST_YGGDRASIL_IP:
                    exchange_peers(target_ip, self.port)
            
            # 2. **Audit & Repair Logic Placeholder:**
            # Here is where the heavy logic from the last step would run
            # to check if "hash_A_chunk0" is hosted by enough peers (R=3).
            
            self.stop_event.wait(GOSSIP_INTERVAL_SECONDS)

    def stop(self):
        self.stop_event.set()


# --- MAIN SERVER STARTUP ---

def start_server():
    # 1. Ensure basic environment setup (directories, etc.)
    ensure_initial_setup()
    
    # 2. AUTO-INDEXER RUNS ONCE AT STARTUP
    auto_index_shared_files()
    
    server_socket = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    
    try:
        # Using the HOST_YGGDRASIL_IP for bind as per your successful debugging, 
        # but recommend ('::', PORT) for reliability on some Windows/Linux setups.
        server_socket.bind((HOST_YGGDRASIL_IP, PORT))
        server_socket.listen(10)
        
        # Start the background gossiper thread
        gossiper = PeerGossiper(PORT)
        gossiper.daemon = True 
        gossiper.start()
        
        print(f"\n✅ Yggdrasil Server running on {HOST_YGGDRASIL_IP}:{PORT}")
        print(f"   Gossip/Audit running every {GOSSIP_INTERVAL_SECONDS}s...")
        
        while True:
            # Main thread accepts connections and hands them off
            client_socket, client_address = server_socket.accept()
            peer_ip = client_address[0]
            
            peer_thread = threading.Thread(
                target=handle_peer_connection, 
                args=(client_socket, peer_ip)
            )
            peer_thread.start()

    except KeyboardInterrupt:
        print("\n[SERVER] Shutdown requested.")
    except Exception as e:
        print(f"❌ Critical server error: {e}")
    finally:
        gossiper.stop()
        server_socket.close()

if __name__ == "__main__":
    start_server()


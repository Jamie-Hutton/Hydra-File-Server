import socket
import json
import os
import sys
import hashlib
import time
from config import *

#try:
#    from config import * except ImportError:
#    print("Error: config.py not found.")
#    sys.exit(1)

# --- CORE CLIENT FUNCTIONS ---

def get_remote_file_list(target_ip):
    """Connects to a peer and requests the full file index."""
    client_socket = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
    try:
        client_socket.connect((target_ip, PORT))
        client_socket.sendall("REQUEST_FILE_LIST".encode('utf-8'))
        
        header_data = client_socket.recv(1024).decode('utf-8').strip()
        if header_data.startswith("LIST_SIZE:"):
            size = int(header_data.split(":")[1])
            client_socket.sendall(b"READY")
            
            # Receive full JSON payload (handle large JSONs in chunks)
            received_data = b""
            while len(received_data) < size:
                packet = client_socket.recv(4096)
                if not packet: break
                received_data += packet
            
            return json.loads(received_data.decode('utf-8'))
        return {}
    except Exception as e:
        print(f"❌ Error getting file list from {target_ip}: {e}")
        return {}
    finally:
        client_socket.close()

def download_chunk(target_ip, filename, chunk_meta):
    """Downloads a single chunk and verifies hash."""
    chunk_id = chunk_meta['id']
    chunk_hash = chunk_meta['hash']
    chunk_size = chunk_meta['size']
    
    # Save to a temporary file
    temp_path = os.path.join(DOWNLOAD_DIRECTORY, f"{filename}.part{chunk_id}")
    
    # Skip if already downloaded and verified (Resume capability)
    if os.path.exists(temp_path):
        if calculate_hash(temp_path) == chunk_hash:
            print(f"   ⏩ Chunk {chunk_id} already exists and is valid. Skipping.")
            return True

    client_socket = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
    try:
        client_socket.connect((target_ip, PORT))
        cmd = f"REQUEST_CHUNK:{filename}:{chunk_id}"
        client_socket.sendall(cmd.encode('utf-8'))
        
        header = client_socket.recv(1024).decode('utf-8').strip()
        if header.startswith("CHUNK_READY:"):
            client_socket.sendall(b"READY")
            
            received_bytes = 0
            hasher = hashlib.sha256()
            
            with open(temp_path, 'wb') as f:
                while received_bytes < chunk_size:
                    data = client_socket.recv(4096)
                    if not data: break
                    f.write(data)
                    hasher.update(data)
                    received_bytes += len(data)
            
            if hasher.hexdigest() == chunk_hash:
                print(f"   ✅ Chunk {chunk_id} downloaded & verified.")
                return True
            else:
                print(f"   ❌ Chunk {chunk_id} hash mismatch!")
                os.remove(temp_path)
                return False
        else:
            print(f"   ❌ Peer error for Chunk {chunk_id}: {header}")
            return False
            
    except Exception as e:
        print(f"   ❌ Exception downloading Chunk {chunk_id}: {e}")
        return False
    finally:
        client_socket.close()

def reassemble_file(filename, chunks_metadata):
    """Stitches all verified chunks back into the original file."""
    print(f"\n🧩 Reassembling '{filename}'...")
    final_path = os.path.join(DOWNLOAD_DIRECTORY, filename)
    
    with open(final_path, 'wb') as outfile:
        # Sort chunks by ID to ensure correct order
        chunks_metadata.sort(key=lambda x: x['id'])
        
        for chunk in chunks_metadata:
            part_path = os.path.join(DOWNLOAD_DIRECTORY, f"{filename}.part{chunk['id']}")
            
            if not os.path.exists(part_path):
                print(f"   ❌ Missing chunk {chunk['id']}. Reassembly failed.")
                return
            
            with open(part_path, 'rb') as infile:
                outfile.write(infile.read())
            
            # Clean up temp part file
            os.remove(part_path)
            
    print(f"🎉 Success! File saved to: {final_path}")

# --- MAIN EXECUTION ---

def start_download_manager():
    ensure_initial_setup()
    peers = load_peers()
    
    # 1. Find a target peer (simple selection for now)
    target_peer = next((p for p in peers if p != HOST_YGGDRASIL_IP), None)
    if not target_peer:
        print("❌ No peers found in known_peers.json.")
        return

    print(f"📡 connecting to {target_peer} to fetch file list...")
    file_index = get_remote_file_list(target_peer)
    
    if not file_index:
        print("   No files found on peer.")
        return

    print("\n📂 Available Files:")
    filenames = list(file_index.keys())
    for i, fname in enumerate(filenames):
        size_mb = file_index[fname]['total_size'] / (1024*1024)
        print(f"   [{i}] {fname} ({size_mb:.2f} MB)")

    # 2. User Selects File
    try:
        choice = int(input("\nEnter number of file to download: "))
        target_filename = filenames[choice]
    except (ValueError, IndexError):
        print("Invalid selection.")
        return

    # 3. Start Download Loop
    file_meta = file_index[target_filename]
    chunks = file_meta['chunks']
    print(f"\n⬇️ Starting download of '{target_filename}' ({len(chunks)} chunks)...")

    # In a real swarm, this loop would be multi-threaded!
    all_chunks_success = True
    for chunk in chunks:
        success = download_chunk(target_peer, target_filename, chunk)
        if not success:
            all_chunks_success = False
            print("🛑 Download aborted due to chunk error.")
            break
    
    # 4. Reassemble
    if all_chunks_success:
        reassemble_file(target_filename, chunks)

if __name__ == "__main__":
    start_download_manager()


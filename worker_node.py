from flask import Flask, request, jsonify
import os
import requests
import socket

app = Flask(__name__)
storage_dir = "./storage"
master_url = "http://10.1.18.121:55000/register"  # Replace <MASTER_IP> with the master node's IP

# Ensure storage directory exists
os.makedirs(storage_dir, exist_ok=True)

def get_private_ip():
    """Get the private IP address of the worker."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
    finally:
        s.close()
    return ip

def register_with_master():
    """Register the worker with the master."""
    worker_ip = get_private_ip()
    worker_port = os.getenv("WORKER_PORT", "55001")
    worker_url = f"http://{worker_ip}:{worker_port}"
    try:
        response = requests.post(master_url, json={"url": worker_url})
        if response.status_code == 200:
            print(f"Registered with master: {worker_url}")
        else:
            print(f"Failed to register with master: {response.json()}")
    except Exception as e:
        print(f"Error registering with master: {e}")

@app.route('/store/<filename>', methods=['POST'])
def store(filename):
    """Store a file chunk under a directory named after the original file name."""
    data = request.json
    chunk_id = data['chunk_id']
    chunk_data = data['data']

    # Create a directory for the original file name
    file_dir = os.path.join(storage_dir, filename)
    os.makedirs(file_dir, exist_ok=True)

    # Save chunk to the file's directory
    chunk_path = os.path.join(file_dir, chunk_id)
    with open(chunk_path, 'w') as f:
        f.write(chunk_data)
    return jsonify({"message": f"Chunk '{chunk_id}' stored successfully in '{filename}'!"})

@app.route('/retrieve/<filename>/<chunk_id>', methods=['GET'])
def retrieve(filename, chunk_id):
    """Retrieve a stored file chunk from its directory."""
    file_dir = os.path.join(storage_dir, filename)
    chunk_path = os.path.join(file_dir, chunk_id)

    if not os.path.exists(chunk_path):
        return jsonify({"error": "Chunk not found!"}), 404

    with open(chunk_path, 'r') as f:
        chunk_data = f.read()
    return jsonify({"chunk_id": chunk_id, "data": chunk_data})

if __name__ == "__main__":
    # Worker environment variables for IP and Port
    worker_ip = os.getenv("WORKER_IP", "127.0.0.1")
    worker_port = os.getenv("WORKER_PORT", "55001")

    # Register with master before starting
    register_with_master()

    app.run(host="0.0.0.0", port=int(worker_port))
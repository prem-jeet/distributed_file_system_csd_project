from flask import Flask, request, jsonify
import requests

app = Flask(__name__)

# Metadata to track file chunks and their locations
metadata = {}  # {filename: [chunk_locations]}
workers = []  # Dynamic list of workers

@app.route('/register', methods=['POST'])
def register_worker():
    """Register a new worker."""
    data = request.json
    worker_url = data.get('url')
    if worker_url and worker_url not in workers:
        workers.append(worker_url)
        return jsonify({"message": f"Worker {worker_url} registered successfully!"}), 200
    return jsonify({"error": "Invalid or duplicate worker URL"}), 400

@app.route('/upload', methods=['POST'])
def upload():
    """Handle file uploads and distribute chunks to workers."""
    data = request.json
    filename = data['filename']
    file_chunks = data['chunks']
    assigned_chunks = []

    if not workers:
        return jsonify({"error": "No workers available"}), 500

    # Assign chunks to workers dynamically
    for i, chunk in enumerate(file_chunks):
        worker = workers[i % len(workers)]  # Round-robin assignment
        worker_store_url = f"{worker}/store/{filename}"  # Include the filename in the URL
        assigned_chunks.append({'url': worker, 'endpoint': worker_store_url, 'chunk_id': chunk['chunk_id']})
        
        # Send chunk to the worker
        response = requests.post(worker_store_url, json=chunk)
        if response.status_code != 200:
            return jsonify({"error": f"Failed to store chunk {chunk['chunk_id']} on worker {worker}"}), 500

    metadata[filename] = assigned_chunks
    return jsonify({"message": f"File '{filename}' uploaded successfully!", "chunks": assigned_chunks})

@app.route('/download/<filename>', methods=['GET'])
def download(filename):
    """Handle file downloads."""
    if filename not in metadata:
        return jsonify({"error": "File not found!"}), 404
    return jsonify({"chunks": metadata[filename]})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=55000)
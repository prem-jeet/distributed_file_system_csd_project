from flask import Flask, request, jsonify
import requests
app = Flask(__name__)

# Metadata to track file chunks and their locations
metadata = {}  # {filename: [chunk_locations]}
workers = ["http://127.0.0.1:55001", "http://127.0.0.1:55002"]  # Worker IPs

@app.route('/upload', methods=['POST'])
def upload():
    data = request.json
    filename = data['filename']
    file_chunks = data['chunks']
    assigned_chunks = []

    # Assign chunks to workers in a round-robin fashion
    for i, chunk in enumerate(file_chunks):
        worker = workers[i % len(workers)]
        assigned_chunks.append({'url': worker, 'endpoint': f"{worker}/store/{chunk['chunk_id']}", 'chunkid':chunk['chunk_id'], })
        # Send chunk to the worker
        response = requests.post(f"{worker}/store", json=chunk)
        if response.status_code != 200:
            return jsonify({"error": "Failed to store chunk"}), 500

    metadata[filename] = assigned_chunks
    return jsonify({"message": f"File '{filename}' uploaded successfully!", "chunks": assigned_chunks})

@app.route('/download/<filename>', methods=['GET'])
def download(filename):
    if filename not in metadata:
        return jsonify({"error": "File not found!"}), 404
    return jsonify({"chunks": metadata[filename]})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=55000)

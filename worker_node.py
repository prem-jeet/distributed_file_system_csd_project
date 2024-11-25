from flask import Flask, request, jsonify
import os

app = Flask(__name__)
storage_dir = "./storage"

# Ensure storage directory exists
os.makedirs(storage_dir, exist_ok=True)

@app.route('/store', methods=['POST'])
def store():
    data = request.json
    chunk_id = data['chunk_id']
    chunk_data = data['data']

    # Save chunk to disk
    with open(os.path.join(storage_dir, chunk_id), 'w') as f:
        f.write(chunk_data)
    return jsonify({"message": f"Chunk '{chunk_id}' stored successfully!"})

@app.route('/retrieve/<chunk_id>', methods=['GET'])
def retrieve(chunk_id):
    file_path = os.path.join(storage_dir, chunk_id)
    if not os.path.exists(file_path):
        return jsonify({"error": "Chunk not found!"}), 404

    with open(file_path, 'r') as f:
        chunk_data = f.read()
    return jsonify({"chunk_id": chunk_id, "data": chunk_data})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=55001)

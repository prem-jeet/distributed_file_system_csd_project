import requests
import os

MASTER_NODE_URL = "http://127.0.0.1:55000"

def split_file(file_path, chunk_size=30):
    chunks = []
    with open(file_path, 'r') as f:
        data = f.read()
        for i in range(0, len(data), chunk_size):
            chunks.append({"chunk_id": f"chunk_{i}", "data": data[i:i+chunk_size]})
    return chunks

def upload_file(file_path):
    filename = os.path.basename(file_path)
    chunks = split_file(file_path)
    print("Chunks to be uploaded:", chunks)
    response = requests.post(f"{MASTER_NODE_URL}/upload", json={"filename": filename, "chunks": chunks})
    
    if response.status_code == 200:
        print("Upload successful:", response.json())
    else:
        print("Failed to upload:", response.json())

def download_file(filename, output_path):
    response = requests.get(f"{MASTER_NODE_URL}/download/{filename}")
    
    if response.status_code != 200:
        print(f"Error: {response.json()}")
        return
    
    # Assuming the response contains a 'chunks' list with URLs to fetch the file parts.
    chunks = response.json().get('chunks', [])
    print(f"Chunks format: {chunks[0]}")
    
    try:
        with open(output_path, 'w') as f:
            for chunk in chunks:
                print(f"Downloading chunk from {chunk['url']}/retrieve/{chunk['chunkid']}")
                chunk_response = requests.get(f"{chunk['url']}/retrieve/{chunk['chunkid']}")
                
                if chunk_response.status_code == 200:
                    chunk_data = chunk_response.json().get('data', '')
                    f.write(chunk_data)  # Write the chunk data to the output file
                    print(f"Downloaded chunk from {chunk['url']}")
                else:
                    print(f"Error downloading chunk from {chunk['url']}. Status code: {chunk_response.status_code}")
        print(f"File '{filename}' downloaded successfully to {output_path}!")
    except Exception as e:
        print(f"Error during file download: {e}")

if __name__ == "__main__":
    print("1. Upload File")
    print("2. Download File")
    choice = input("Enter choice: ")

    if choice == "1":
        file_path = input("Enter file path to upload: ")
        upload_file(file_path)
    elif choice == "2":
        filename = input("Enter filename to download: ")
        output_path = input("Enter output file path: ")
        download_file(filename, output_path)

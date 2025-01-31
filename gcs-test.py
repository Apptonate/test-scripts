import subprocess
import argparse
import os
import hashlib
import logging
from datetime import datetime
from google.cloud import storage

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def run_command(command, error_message):
    try:
        result = subprocess.run(command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        logging.info(f"Command '{' '.join(command)}' executed successfully.")
        return True
    except subprocess.CalledProcessError as e:
        logging.error(f"Error: {error_message}")
        logging.error(f"Command: {' '.join(command)}")
        logging.error(f"Return code: {e.returncode}")
        logging.error(f"Output: {e.stdout}")
        logging.error(f"Error: {e.stderr}")
        return None

def calculate_checksum(file_path):
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    logging.info(f"Calculated checksum for {file_path}.")
    return sha256_hash.hexdigest()

def verify_clone(repo_path, expected_checksum, expected_size):
    # This function can be expanded to perform actual verification if needed.
    logging.info("Verifying cloned repository (this is a placeholder).")
    return True

def upload_to_gcs(local_directory, bucket_name, destination_blob_name):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    for root, _, files in os.walk(local_directory):
        for file in files:
            local_file_path = os.path.join(root, file)
            relative_path = os.path.relpath(local_file_path, local_directory)
            blob_path = os.path.join(destination_blob_name, relative_path)
            blob = bucket.blob(blob_path)
            blob.upload_from_filename(local_file_path)
            logging.info(f"Uploaded {local_file_path} to gs://{bucket_name}/{blob_path}")

def main(repo_url, bucket_name):
    if not run_command(["git", "lfs", "install"], "Failed to install git-lfs"):
        return

    # Step 1: Create a temporary local directory with a timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    repo_name = repo_url.split('/')[-1].replace('.git', '')
    local_dir = f"{repo_name}_{timestamp}"
    os.makedirs(local_dir, exist_ok=True)
    logging.info(f"Created local directory {local_dir}.")

    # Step 2: Clone the repository locally
    clone_output = run_command(["git", "clone", repo_url, local_dir], f"Failed to clone repository from {repo_url}")
    if clone_output:
        lfs_clone = run_command(["git", "-C", local_dir, "lfs", "pull"], f"Failed to pull Git LFS files in {local_dir}")
    else:
        logging.error("-E clone failed")
        raise

    # Step 3: Verify the clone (if needed)
    if verify_clone(local_dir, {}, 0):  # Empty checksum and size for placeholder
        logging.info("Repository cloned and verified successfully.")
    else:
        logging.error("Repository verification failed.")
        return

    # Step 4: Upload to GCS
    upload_to_gcs(local_dir, bucket_name, os.path.basename(local_dir))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Clone a Git repository and upload to a GCS bucket.")
    parser.add_argument("repo_url", help="URL of the repository to clone")
    parser.add_argument("bucket_name", help="GCS bucket name where the repository should be uploaded")
    args = parser.parse_args()
    main(args.repo_url, args.bucket_name)

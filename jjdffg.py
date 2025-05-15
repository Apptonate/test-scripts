#!/usr/bin/env python3
"""
Simplified JFrog File Uploader
------------------------------
Uploads files to JFrog Artifactory with parallel processing for small files
and sequential processing for large files.
"""

import os
import sys
import hashlib
import argparse
import logging
import time
import requests
import concurrent.futures
import threading
import psutil
from pathlib import Path
from tqdm import tqdm

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("jfrog_uploader.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("JFrogUploader")

# Constants
DEFAULT_CONCURRENT_UPLOADS = 3
DEFAULT_STREAMS = 5  # Default number of streams for calculating chunk size
DEFAULT_CHUNK_SIZE = 4 * 1024 * 1024  # 4MB chunks default (used only if adaptive calculation fails)
DEFAULT_LARGE_FILE_THRESHOLD = 100 * 1024 * 1024  # 100MB default threshold
MAX_RETRIES = 3
RETRY_BACKOFF_FACTOR = 1.0
CONNECTION_TIMEOUT = 30
READ_TIMEOUT = 300
MIN_CHUNK_SIZE = 1 * 1024 * 1024  # 1MB minimum chunk size
MAX_CHUNK_SIZE = 64 * 1024 * 1024  # 64MB maximum chunk size

def calculate_optimal_chunk_size(streams=DEFAULT_STREAMS):
    """
    Calculate optimal chunk size based on available system memory.
    
    Args:
        streams: Number of concurrent streams to allocate memory for
        
    Returns:
        int: Chunk size in bytes
    """
    try:
        # Get system memory information
        mem = psutil.virtual_memory()
        total_mem = mem.total
        available_mem = mem.available
        
        # Use available memory, but if it's very low, use a percentage of total
        usable_mem = available_mem
        if available_mem < total_mem * 0.1:  # If less than 10% available
            usable_mem = total_mem * 0.1  # Use 10% of total memory
        
        # Reserve 20% for system and other processes
        usable_mem = usable_mem * 0.8
        
        # Divide by number of concurrent streams
        mem_per_stream = usable_mem / streams
        
        # Use 25% of that for a single chunk (to allow for overhead)
        chunk_size = int(mem_per_stream * 0.25)
        
        # Clamp to reasonable min/max values
        chunk_size = max(MIN_CHUNK_SIZE, min(MAX_CHUNK_SIZE, chunk_size))
        
        # Round to nearest MB for cleaner numbers
        chunk_size = (chunk_size // (1024 * 1024)) * (1024 * 1024)
        
        logger.info(f"Calculated optimal chunk size: {chunk_size / (1024 * 1024):.1f} MB based on {usable_mem / (1024 * 1024):.1f} MB usable memory and {streams} streams")
        return chunk_size
    except Exception as e:
        logger.warning(f"Failed to calculate optimal chunk size: {str(e)}. Using default size.")
        return DEFAULT_CHUNK_SIZE

class UploadTracker:
    """Tracks progress for a file upload."""
    def __init__(self, file_path, total_size):
        self.file_path = file_path
        self.total_size = total_size
        self.uploaded = 0
        self.start_time = time.time()
        self.lock = threading.Lock()
        self.progress_bar = tqdm(total=total_size, unit='B', unit_scale=True, desc=os.path.basename(file_path))

    def update(self, chunk_size):
        with self.lock:
            self.uploaded += chunk_size
            self.progress_bar.update(chunk_size)

    def close(self):
        try:
            self.progress_bar.close()
            elapsed = time.time() - self.start_time
            rate = self.uploaded / elapsed / 1024 / 1024 if elapsed > 0 else 0
            logger.info(f"Upload of {os.path.basename(self.file_path)} completed in {elapsed:.2f}s "
                       f"({rate:.2f} MB/s average)")
        except:
            pass

class JFrogUploader:
    """Handles uploading files to JFrog Artifactory."""
    
    def __init__(self, base_url, repo_name, username, api_key, max_workers=DEFAULT_CONCURRENT_UPLOADS, 
                 large_threshold=DEFAULT_LARGE_FILE_THRESHOLD, chunk_size=None):
        self.base_url = base_url.rstrip('/')
        self.repo_name = repo_name
        self.auth = (username, api_key)
        self.max_workers = max_workers
        self.large_threshold = large_threshold
        
        # Use provided chunk size or calculate optimal size
        if chunk_size is None:
            self.chunk_size = calculate_optimal_chunk_size(streams=max(max_workers, DEFAULT_STREAMS))
        else:
            self.chunk_size = chunk_size
            
        self.session = requests.Session()
        self.large_file_lock = threading.Lock()
    
    def calculate_md5(self, file_path):
        """Calculate MD5 hash of a file."""
        logger.debug(f"Calculating MD5 for {file_path}")
        md5 = hashlib.md5()
        
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(8192), b''):
                md5.update(chunk)
        
        result = md5.hexdigest()
        logger.debug(f"MD5 hash: {result}")
        return result
    
    def upload_file(self, file_path, target_path=None):
        """Upload a single file to JFrog Artifactory."""
        file_path = Path(file_path)
        if not file_path.exists():
            logger.error(f"File not found: {file_path}")
            return False
        
        file_size = file_path.stat().st_size
        logger.info(f"Starting upload for {file_path} ({file_size/1024/1024:.2f} MB)")
        
        # If target path not specified, use the file name
        if target_path is None:
            target_path = file_path.name
            
        # Remove leading slash if present
        target_path = target_path.lstrip('/')
        
        # Construct the full URL
        url = f"{self.base_url}/artifactory/{self.repo_name}/{target_path}"
        
        # Initialize progress tracker
        tracker = UploadTracker(str(file_path), file_size)
        
        try:
            # Calculate MD5 for all files
            md5_hash = self.calculate_md5(file_path)
            
            # Upload based on file size
            if file_size >= self.large_threshold:
                logger.info(f"File size {file_size/1024/1024:.2f} MB exceeds threshold of {self.large_threshold/1024/1024:.2f} MB, processing sequentially")
                # Lock for large files to process one at a time
                with self.large_file_lock:
                    success = self._upload_with_streaming(file_path, url, file_size, tracker, md5_hash)
            else:
                # Small files can be uploaded in parallel
                success = self._upload_with_streaming(file_path, url, file_size, tracker, md5_hash)
            
            return success
        except Exception as e:
            logger.error(f"Upload failed for {file_path}: {str(e)}")
            tracker.close()
            return False
    
    def _upload_with_streaming(self, file_path, url, file_size, tracker, md5_hash):
        """Upload a file with streaming and chunks to track progress."""
        logger.info(f"Uploading {file_path} with streaming (chunk size: {self.chunk_size/1024:.0f}KB)")
        
        # Create headers with MD5 checksum
        headers = {
            'Content-Type': 'application/octet-stream',
            'X-Checksum-Md5': md5_hash
        }
        
        # Log the headers being used
        logger.debug(f"Request headers: {headers}")
        
        # Create a generator to stream file content in chunks while tracking progress
        def file_content_generator():
            with open(file_path, 'rb') as f:
                while True:
                    chunk = f.read(self.chunk_size)
                    if not chunk:
                        break
                    tracker.update(len(chunk))
                    yield chunk
        
        # Upload with retries
        for retry in range(MAX_RETRIES):
            try:
                # Use a streaming upload
                with self.session.put(
                    url,
                    data=file_content_generator(),
                    headers=headers,
                    auth=self.auth,
                    timeout=(CONNECTION_TIMEOUT, READ_TIMEOUT)
                ) as response:
                    if response.status_code in (200, 201):
                        tracker.close()
                        logger.info(f"Upload successful for {file_path}")
                        return self._validate_upload(url, file_path)
                    else:
                        logger.warning(
                            f"Upload failed, retry {retry + 1}/{MAX_RETRIES}. "
                            f"Status: {response.status_code}, Response: {response.text}"
                        )
                        
                        # If we get a checksum error, try with a different header
                        if "checksum" in response.text.lower():
                            logger.warning(f"Checksum error detected! Adding X-Checksum-Deploy header")
                            headers['X-Checksum-Deploy'] = 'true'
                        
                        # Wait longer for each retry
                        backoff_time = RETRY_BACKOFF_FACTOR * (2 ** retry)
                        logger.info(f"Waiting {backoff_time}s before retry...")
                        time.sleep(backoff_time)
            except Exception as e:
                logger.warning(f"Upload exception, retry {retry + 1}/{MAX_RETRIES}: {str(e)}")
                backoff_time = RETRY_BACKOFF_FACTOR * (2 ** retry)
                logger.info(f"Waiting {backoff_time}s before retry...")
                time.sleep(backoff_time)
        
        tracker.close()
        logger.error(f"Failed to upload {file_path} after {MAX_RETRIES} retries")
        return False
    
    def _validate_upload(self, url, file_path):
        """Validate the uploaded file exists and has correct size."""
        logger.info(f"Validating upload for {url}")
        
        try:
            # Get file info from JFrog
            response = self.session.head(url, auth=self.auth, timeout=(CONNECTION_TIMEOUT, 30))
            
            if response.status_code != 200:
                logger.error(f"Validation failed: Could not retrieve file info. Status: {response.status_code}")
                return False
                
            # File exists on server - basic validation passed
            logger.info("File exists on server")
            
            # Check file size
            local_file_size = os.path.getsize(str(file_path))
            if 'Content-Length' in response.headers:
                server_file_size = int(response.headers['Content-Length'])
                
                if server_file_size == local_file_size:
                    logger.info(f"File size matches: {server_file_size} bytes")
                    return True
                else:
                    logger.warning(f"File size mismatch - Local: {local_file_size}, Server: {server_file_size}")
                    return False
            
            # If we can't verify size, just assume it worked
            return True
                
        except Exception as e:
            logger.error(f"Validation error: {str(e)}")
            return False
    
    def upload_directory(self, directory_path, target_prefix=None):
        """Upload all files in a directory with parallel processing."""
        directory_path = Path(directory_path)
        if not directory_path.is_dir():
            logger.error(f"Directory not found: {directory_path}")
            return False
        
        # Collect all files
        files = []
        for item in directory_path.glob('**/*'):
            if item.is_file():
                # Calculate relative path
                rel_path = item.relative_to(directory_path)
                if target_prefix:
                    target_path = f"{target_prefix}/{rel_path}"
                else:
                    target_path = str(rel_path)
                files.append((str(item), target_path))
        
        total_files = len(files)
        if total_files == 0:
            logger.warning(f"No files found in {directory_path}")
            return True
        
        logger.info(f"Found {total_files} files to upload")
        
        # Sort files by size (smallest first)
        files.sort(key=lambda x: os.path.getsize(x[0]))
        
        # Calculate total size
        total_size = sum(os.path.getsize(file_path) for file_path, _ in files)
        logger.info(f"Total upload size: {total_size/1024/1024:.2f} MB")
        
        # Separate files by size
        large_files = []
        normal_files = []
        
        for file_path, target_path in files:
            file_size = os.path.getsize(file_path)
            if file_size >= self.large_threshold:
                large_files.append((file_path, target_path))
            else:
                normal_files.append((file_path, target_path))
        
        logger.info(f"Files categorized by size threshold of {self.large_threshold/1024/1024:.2f} MB: {len(normal_files)} normal, {len(large_files)} large")
        
        # Process normal files in parallel
        if normal_files:
            logger.info(f"Processing {len(normal_files)} normal files with concurrency {self.max_workers}")
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                # Submit all upload tasks
                future_to_file = {
                    executor.submit(self.upload_file, file_path, target_path): (file_path, target_path)
                    for file_path, target_path in normal_files
                }
                
                # Process results as they complete
                for future in concurrent.futures.as_completed(future_to_file):
                    file_path, target_path = future_to_file[future]
                    try:
                        success = future.result()
                        if success:
                            logger.info(f"Successfully uploaded {file_path} to {target_path}")
                        else:
                            logger.error(f"Failed to upload {file_path} to {target_path}")
                    except Exception as e:
                        logger.error(f"Exception during upload of {file_path}: {str(e)}")
        
        # Process large files one at a time
        if large_files:
            logger.info(f"Processing {len(large_files)} large files one at a time")
            for file_path, target_path in large_files:
                logger.info(f"Processing large file: {file_path}")
                success = self.upload_file(file_path, target_path)
                if success:
                    logger.info(f"Successfully uploaded large file {file_path} to {target_path}")
                else:
                    logger.error(f"Failed to upload large file {file_path} to {target_path}")
        
        logger.info(f"Upload process completed")
        return True

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Upload files to JFrog Artifactory')
    
    parser.add_argument('--url', required=True, help='JFrog Artifactory base URL (e.g., http://localhost:8081)')
    parser.add_argument('--repo', required=True, help='Repository name')
    parser.add_argument('--username', required=True, help='Username for authentication')
    parser.add_argument('--apikey', required=True, help='API key for authentication')
    parser.add_argument('--source', required=True, help='Source directory or file to upload')
    parser.add_argument('--target', help='Target path in the repository')
    parser.add_argument('--parallel', type=int, default=DEFAULT_CONCURRENT_UPLOADS, 
                        help=f'Number of concurrent uploads (default: {DEFAULT_CONCURRENT_UPLOADS})')
    parser.add_argument('--large-threshold', type=float, default=DEFAULT_LARGE_FILE_THRESHOLD/(1024*1024),
                        help=f'Size threshold in MB for large files to process sequentially (default: {DEFAULT_LARGE_FILE_THRESHOLD/(1024*1024):.0f})')
    parser.add_argument('--chunk-size', type=int, default=None,
                        help='Chunk size in KB for streaming uploads (default: auto-calculated based on system memory)')
    
    return parser.parse_args()

def main():
    """Main entry point for the script."""
    args = parse_arguments()
    
    # Convert MB to bytes for the large file threshold
    large_threshold_bytes = int(args.large_threshold * 1024 * 1024)
    
    # Convert KB to bytes for the chunk size if provided
    chunk_size_bytes = None
    if args.chunk_size is not None:
        chunk_size_bytes = int(args.chunk_size * 1024)
    
    logger.info(f"JFrog Upload started")
    logger.info(f"Uploading from {args.source} to {args.repo}/{args.target if args.target else ''}")
    logger.info(f"Using {args.parallel} concurrent uploads")
    logger.info(f"Files larger than {args.large_threshold:.2f} MB will be processed sequentially")
    
    uploader = JFrogUploader(
        base_url=args.url,
        repo_name=args.repo,
        username=args.username,
        api_key=args.apikey,
        max_workers=args.parallel,
        large_threshold=large_threshold_bytes,
        chunk_size=chunk_size_bytes
    )
    
    logger.info(f"Using chunk size of {uploader.chunk_size / (1024 * 1024):.2f} MB for streaming uploads")
    
    source_path = Path(args.source)
    success = False
    
    try:
        if source_path.is_dir():
            success = uploader.upload_directory(source_path, args.target)
        elif source_path.is_file():
            success = uploader.upload_file(source_path, args.target)
        else:
            logger.error(f"Source path does not exist: {source_path}")
            sys.exit(1)
    except KeyboardInterrupt:
        logger.warning("Upload interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        sys.exit(1)
    
    if success:
        logger.info("Upload completed successfully")
        sys.exit(0)
    else:
        logger.error("Upload failed")
        sys.exit(1)

if __name__ == "__main__":
    main()

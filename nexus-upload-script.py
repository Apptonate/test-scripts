#!/usr/bin/env python3
"""
Nexus Repository Zip and Upload Script

This script:
1. Scans a specified local directory for files
2. Creates a zip archive of those files
3. Uploads the zip archive to Nexus Repository
"""

import os
import zipfile
import requests
import argparse
import datetime
import logging
from pathlib import Path
from os import environ

import time
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

# Import tqdm for progress bars
try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("nexus_upload.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("nexus_uploader")

def create_zip_archive(source_dir, output_path=None, chunk_size=8192, max_memory_mb=100):
    """
    Create a ZIP archive of all files in the source directory using memory-efficient streaming.
    
    Args:
        source_dir (str): Directory containing files to zip
        output_path (str, optional): Path for the output ZIP file. 
                                    If None, uses timestamp-based name in current directory.
        chunk_size (int): Size of chunks to read when processing files (bytes)
        max_memory_mb (int): Maximum memory allocation for file collection (MB)
    
    Returns:
        str: Path to the created ZIP file
    """
    source_path = Path(source_dir)
    
    # Generate ZIP filename based on timestamp if not provided
    if not output_path:
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = f"files_{timestamp}.zip"
    
    # Ensure source directory exists
    if not source_path.exists() or not source_path.is_dir():
        raise ValueError(f"Source directory does not exist: {source_dir}")
    
    # Validate the directory has files without loading everything into memory
    file_count = 0
    total_size = 0
    largest_file = {"path": None, "size": 0}
    
    # Process files in smaller batches to avoid memory issues
    batch_size = int((max_memory_mb * 1024 * 1024) / 1000)  # Approximate memory for path objects
    logger.info(f"Scanning directory {source_dir} in batches...")
    
    for i, file_path in enumerate(source_path.glob("**/*")):
        if not file_path.is_file():
            continue
            
        file_count += 1
        file_size = file_path.stat().st_size
        total_size += file_size
        
        # Track largest file for memory estimation
        if file_size > largest_file["size"]:
            largest_file = {"path": file_path, "size": file_size}
            
        # Log progress in batches
        if file_count % 1000 == 0:
            logger.info(f"Scanned {file_count} files so far...")
    
    if file_count == 0:
        raise ValueError(f"No files found in source directory: {source_dir}")
    
    # Log summary before zipping
    total_size_mb = total_size / (1024 * 1024)
    largest_file_mb = largest_file["size"] / (1024 * 1024)
    logger.info(f"Found {file_count} files ({total_size_mb:.2f} MB total) to compress")
    logger.info(f"Largest file is {largest_file_mb:.2f} MB: {largest_file['path'].relative_to(source_path)}")
    
    # Create the ZIP file using streaming to minimize memory usage with ZIP64 support explicitly enabled
    with zipfile.ZipFile(output_path, 'w', zipfile.ZIP_DEFLATED, allowZip64=True) as zipf:
        # Create a file list for tracking progress
        files_list = [f for f in source_path.glob("**/*") if f.is_file()]
        
        # Setup progress bar if tqdm is available
        if TQDM_AVAILABLE:
            pbar = tqdm(total=len(files_list), unit='file', desc="Creating ZIP")
        
        for file_path in files_list:
            # Use relative path inside the ZIP file
            arcname = file_path.relative_to(source_path)
            file_size = file_path.stat().st_size
            
            try:
                # Improved handling for large files - use chunked approach
                if file_size > 1 * 1024 * 1024 * 1024:  # If file is larger than 1GB
                    logger.info(f"Processing large file with chunked approach: {arcname} ({file_size/(1024*1024*1024):.2f} GB)")
                    
                    # Create a ZipInfo object with explicit ZIP64 support
                    zi = zipfile.ZipInfo(str(arcname))
                    zi.compress_type = zipfile.ZIP_DEFLATED
                    zi.file_size = file_size  # Set size to ensure ZIP64 headers
                    
                    # Use larger chunk size for better performance with large files
                    large_chunk_size = chunk_size * 16  # Adjust based on available memory
                    
                    # Open the file for writing to the ZIP with the ZipInfo object
                    with zipf.open(zi, 'w', force_zip64=True) as dest:
                        with open(file_path, 'rb') as source:
                            # Copy in chunks to avoid memory issues
                            bytes_processed = 0
                            while True:
                                chunk = source.read(large_chunk_size)
                                if not chunk:
                                    break
                                dest.write(chunk)
                                bytes_processed += len(chunk)
                                
                                # Log progress for very large files
                                if bytes_processed % (100 * 1024 * 1024) < large_chunk_size:  # Log every ~100MB
                                    logger.info(f"  Progress: {bytes_processed/(1024*1024):.1f} MB / {file_size/(1024*1024):.1f} MB ({bytes_processed/file_size*100:.1f}%)")
                else:
                    # For smaller files, use writestr
                    with open(file_path, 'rb') as f:
                        zipf.writestr(str(arcname), f.read(), compress_type=zipfile.ZIP_DEFLATED)
            
            except Exception as e:
                logger.error(f"Error adding file {arcname}: {str(e)}")
                raise
            
            # Update progress bar
            if TQDM_AVAILABLE:
                pbar.update(1)
        
        # Close progress bar
        if TQDM_AVAILABLE:
            pbar.close()
    
    zip_size = Path(output_path).stat().st_size / (1024 * 1024)  # Size in MB
    compression_ratio = 0 if total_size == 0 else (1 - (zip_size * 1024 * 1024) / total_size) * 100
    logger.info(f"Created ZIP archive: {output_path} ({zip_size:.2f} MB, {compression_ratio:.1f}% compression)")
    
    return output_path

def upload_to_nexus(zip_file, nexus_url, repository, username, password, directory=None, chunk_size=1024*1024):
    """
    Upload a ZIP file to Nexus repository using streaming to minimize memory usage.
    
    Args:
        zip_file (str): Path to the ZIP file to upload
        nexus_url (str): Base URL of the Nexus repository
        repository (str): Name of the repository
        username (str): Nexus username
        password (str): Nexus password
        directory (str, optional): Target directory in Nexus repository
        chunk_size (int): Size of chunks for streaming upload (bytes)
    
    Returns:
        bool: True if upload was successful, False otherwise
    """
    zip_path = Path(zip_file)
    
    # Ensure ZIP file exists
    if not zip_path.exists():
        raise ValueError(f"ZIP file does not exist: {zip_file}")
    
    # Get file size for progress tracking
    file_size = zip_path.stat().st_size
    file_size_mb = file_size / (1024 * 1024)
    
    # Prepare upload URL
    filename = zip_path.name
    upload_url = f"{nexus_url.rstrip('/')}/repository/{repository}"
    
    # Add directory to path if specified
    if directory:
        upload_url = f"{upload_url}/{directory.strip('/')}"
    
    upload_url = f"{upload_url}/{filename}"
    
    logger.info(f"Uploading {zip_file} ({file_size_mb:.2f} MB) to {upload_url}")
    
    # Set up a requests session with retries and longer timeouts
    session = requests.Session()
    retries = Retry(
        total=5,  # Total number of retries
        backoff_factor=1,  # Exponential backoff
        status_forcelist=[500, 502, 503, 504],  # Retry on these status codes
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    
    # Set longer timeouts
    # (connect timeout, read timeout) in seconds - 30 min read timeout for large files
    timeout = (30, 1800)
    
    try:
        with open(zip_file, 'rb') as file_data:
            # Setup progress bar for upload if tqdm is available
            if TQDM_AVAILABLE:
                pbar = tqdm(total=file_size, unit='B', unit_scale=True, desc="Uploading to Nexus")
                
                # Create a generator that yields file chunks with progress updates
                def file_chunks():
                    bytes_read = 0
                    start_time = time.time()
                    
                    while True:
                        chunk = file_data.read(chunk_size)
                        if not chunk:
                            break
                        
                        bytes_read += len(chunk)
                        pbar.update(len(chunk))
                        
                        # Log additional progress info periodically
                        if bytes_read % (50 * chunk_size) == 0:
                            elapsed = time.time() - start_time
                            speed_mb = (bytes_read / elapsed) / (1024 * 1024) if elapsed > 0 else 0
                            logger.info(f"Upload in progress: {bytes_read/(1024*1024):.2f} MB sent at {speed_mb:.2f} MB/s")
                        
                        yield chunk
            else:
                # Create a generator without progress bar but with console logging
                def file_chunks():
                    bytes_read = 0
                    last_progress = 0
                    start_time = time.time()
                    
                    while True:
                        chunk = file_data.read(chunk_size)
                        if not chunk:
                            break
                        
                        bytes_read += len(chunk)
                        elapsed = time.time() - start_time
                        progress = int((bytes_read / file_size) * 100)
                        
                        # Log progress every 5% or at least every 30 seconds
                        current_time = time.time()
                        if progress >= last_progress + 5 or current_time - start_time - elapsed >= 30:
                            speed = bytes_read / elapsed if elapsed > 0 else 0
                            speed_mb = speed / (1024 * 1024)
                            eta_seconds = (file_size - bytes_read) / speed if speed > 0 else 0
                            eta_minutes = eta_seconds / 60
                            
                            logger.info(
                                f"Upload progress: {progress}% ({bytes_read/(1024*1024):.2f} MB / {file_size_mb:.2f} MB) "
                                f"- Speed: {speed_mb:.2f} MB/s - ETA: {eta_minutes:.1f} minutes"
                            )
                            last_progress = progress
                        
                        yield chunk
            
            # Explicitly set headers for the upload
            headers = {
                'Content-Type': 'application/zip',
                'Accept': '*/*'
            }
            
            # Log the start of the actual HTTP request
            logger.info(f"Starting HTTP PUT request to {upload_url}")
            
            # Upload to Nexus with extended timeout
            response = session.put(
                upload_url,
                data=file_chunks(),
                auth=(username, password),
                headers=headers,
                timeout=timeout
            )
        
        # Close progress bar if used
        if TQDM_AVAILABLE:
            pbar.close()
            
        # Check response
        if response.status_code in [200, 201]:
            logger.info(f"Upload successful! Status code: {response.status_code}")
            return True
        else:
            logger.error(f"Upload failed! Status code: {response.status_code}")
            logger.error(f"Response: {response.text}")
            return False
            
    except requests.exceptions.Timeout:
        logger.error("Upload timed out. This may indicate the Nexus server is struggling with large files.")
        logger.error("Try increasing timeouts in Nexus configuration or reducing file size.")
        return False
    except requests.exceptions.ConnectionError as e:
        logger.error(f"Connection error: {str(e)}")
        logger.error("Check network stability and Nexus server status.")
        return False
    except Exception as e:
        logger.error(f"Upload error: {str(e)}", exc_info=True)
        return False

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Zip files and upload to Nexus repository")
    parser.add_argument("--source", required=True, help="Source directory containing files to zip")
    parser.add_argument("--zip-file", help="Custom name for the ZIP file (default: timestamp-based)")
    parser.add_argument("--nexus-url", required=True, help="Nexus repository base URL")
    parser.add_argument("--repository", required=True, help="Nexus repository name")
    parser.add_argument("--username", help="Nexus username (or use NEXUS_USERNAME env variable)")
    parser.add_argument("--password", help="Nexus password (or use NEXUS_PASSWORD env variable)")
    parser.add_argument("--directory", help="Target directory in Nexus repository")
    parser.add_argument("--keep-zip", action="store_true", help="Keep the ZIP file after upload")
    parser.add_argument("--chunk-size", type=int, default=1024*1024, help="Chunk size for file operations in bytes (default: 1MB)")
    parser.add_argument("--max-memory", type=int, default=100, help="Maximum memory usage in MB (default: 100MB)")
    parser.add_argument("--temp-dir", help="Custom temporary directory for processing large files")
    parser.add_argument("--max-zip-size", type=float, default=None, help="Maximum size for zip files in GB (for splitting large files)")
    parser.add_argument("--split-archives", action="store_true", help="Split archives if they contain very large files")
    
    args = parser.parse_args()
    
    try:
        # Set custom temp directory if specified
        if args.temp_dir:
            temp_dir = Path(args.temp_dir)
            if not temp_dir.exists():
                temp_dir.mkdir(parents=True, exist_ok=True)
            logger.info(f"Using custom temporary directory: {args.temp_dir}")
            os.environ['TMPDIR'] = args.temp_dir
        
        # Step 1: Create ZIP archive with memory optimization
        zip_file = create_zip_archive(
            args.source, 
            args.zip_file,
            chunk_size=args.chunk_size,
            max_memory_mb=args.max_memory
        )
        
        # Check for tqdm package
        if not TQDM_AVAILABLE:
            logger.info("For progress bars, install the tqdm package: pip install tqdm")
        
        # Get credentials from args or environment variables
        username = args.username or environ.get('NEXUS_USERNAME')
        password = args.password or environ.get('NEXUS_PASSWORD')
        
        # Validate credentials are available
        if not username or not password:
            logger.error("Nexus credentials not provided. Use --username/--password args or set NEXUS_USERNAME/NEXUS_PASSWORD environment variables")
            return 1
        
        # Step 2: Upload to Nexus with streaming
        upload_success = upload_to_nexus(
            zip_file, 
            args.nexus_url, 
            args.repository, 
            username, 
            password, 
            args.directory,
            chunk_size=args.chunk_size
        )
        
        # Step 3: Clean up if requested and upload was successful
        if upload_success and not args.keep_zip:
            os.remove(zip_file)
            logger.info(f"Removed local ZIP file: {zip_file}")
        
        logger.info("Process completed successfully")
        return 0
    
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}", exc_info=True)
        return 1

if __name__ == "__main__":
    exit(main())
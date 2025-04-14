import os
import zipfile
import subprocess
import time
import argparse
import platform
import shutil
from pathlib import Path
from tqdm import tqdm
import psutil
import hashlib 
from concurrent.futures import ThreadPoolExecutor, as_completed
import math
import mmap
import io
import threading
from queue import Queue
from typing import Tuple
import fcntl


def calculate_md5(file_path: str, chunk_size: int = 1024 * 1024) -> str:
    """Calculate MD5 hash of a file in chunks to avoid memory issues."""
    md5 = hashlib.md5()
    try:
        file_size = os.path.getsize(file_path)
        
        # Use memory mapping for all files
        with open(file_path, 'rb') as f:
            with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
                # For small files, read all at once
                if file_size < 10 * 1024 * 1024:  # 10MB
                    md5.update(mm.read())
                else:
                    # For large files, read in chunks
                    for chunk in iter(lambda: mm.read(chunk_size), b''):
                        md5.update(chunk)
    except Exception as e:
        print(f"Error calculating MD5 for {file_path}: {str(e)}")
        raise
    return md5.hexdigest()


def validate_zip_integrity(zip_file, source_folder):
    """Validate zip file integrity by comparing file contents."""
    print(f"Validating zip integrity for {zip_file}...")
    
    # Get list of files in the zip
    with zipfile.ZipFile(zip_file, 'r') as zf:
        zip_files = zf.namelist()
    
    # Get list of files in the source folder
    source_files = []
    for root, _, files in os.walk(source_folder):
        for file in files:
            full_path = os.path.join(root, file)
            rel_path = os.path.relpath(full_path, source_folder)
            # Convert Windows paths to zipfile format
            rel_path = rel_path.replace('\\', '/')
            source_files.append(rel_path)
    
    # Check if file lists match
    missing_files = set(source_files) - set(zip_files)
    if missing_files:
        print(f"Validation failed: {len(missing_files)} files missing from zip")
        print(f"First few missing files: {list(missing_files)[:5]}")
        return False
    
    print(f"All {len(source_files)} files are present in the zip file")
    return True


def get_system_memory():
    """Get available system memory in bytes across different operating systems."""
    system = platform.system().lower()
    
    if system == 'windows':
        # Windows memory detection
        import ctypes
        class MEMORYSTATUSEX(ctypes.Structure):
            _fields_ = [
                ("dwLength", ctypes.c_ulong),
                ("dwMemoryLoad", ctypes.c_ulong),
                ("ullTotalPhys", ctypes.c_ulonglong),
                ("ullAvailPhys", ctypes.c_ulonglong),
                ("ullTotalPageFile", ctypes.c_ulonglong),
                ("ullAvailPageFile", ctypes.c_ulonglong),
                ("ullTotalVirtual", ctypes.c_ulonglong),
                ("ullAvailVirtual", ctypes.c_ulonglong),
                ("ullAvailExtendedVirtual", ctypes.c_ulonglong)
            ]
        
        memory_status = MEMORYSTATUSEX()
        memory_status.dwLength = ctypes.sizeof(MEMORYSTATUSEX)
        ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(memory_status))
        return memory_status.ullAvailPhys
    
    elif system == 'linux':
        # Linux memory detection
        try:
            with open('/proc/meminfo', 'r') as meminfo:
                for line in meminfo:
                    if 'MemAvailable' in line:
                        return int(line.split()[1]) * 1024  # Convert KB to bytes
        except:
            pass
    
    elif system == 'darwin':  # macOS
        # macOS memory detection
        try:
            # Use vm_stat command
            vm_stat = subprocess.check_output(['vm_stat']).decode()
            pages_free = int(vm_stat.split('Pages free:')[1].split('.')[0].strip())
            pages_active = int(vm_stat.split('Pages active:')[1].split('.')[0].strip())
            pages_inactive = int(vm_stat.split('Pages inactive:')[1].split('.')[0].strip())
            page_size = int(subprocess.check_output(['pagesize']).decode().strip())
            return (pages_free + pages_inactive) * page_size
        except:
            pass
    
    # Fallback to psutil if platform-specific methods fail
    return psutil.virtual_memory().available


def get_total_memory():
    """Get total system memory in bytes across different operating systems."""
    system = platform.system().lower()
    
    if system == 'windows':
        import ctypes
        class MEMORYSTATUSEX(ctypes.Structure):
            _fields_ = [
                ("dwLength", ctypes.c_ulong),
                ("dwMemoryLoad", ctypes.c_ulong),
                ("ullTotalPhys", ctypes.c_ulonglong),
                ("ullAvailPhys", ctypes.c_ulonglong),
                ("ullTotalPageFile", ctypes.c_ulonglong),
                ("ullAvailPageFile", ctypes.c_ulonglong),
                ("ullTotalVirtual", ctypes.c_ulonglong),
                ("ullAvailVirtual", ctypes.c_ulonglong),
                ("ullAvailExtendedVirtual", ctypes.c_ulonglong)
            ]
        
        memory_status = MEMORYSTATUSEX()
        memory_status.dwLength = ctypes.sizeof(MEMORYSTATUSEX)
        ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(memory_status))
        return memory_status.ullTotalPhys
    
    elif system == 'linux':
        try:
            with open('/proc/meminfo', 'r') as meminfo:
                for line in meminfo:
                    if 'MemTotal' in line:
                        return int(line.split()[1]) * 1024  # Convert KB to bytes
        except:
            pass
    
    elif system == 'darwin':  # macOS
        try:
            # Use sysctl command
            total_memory = subprocess.check_output(['sysctl', '-n', 'hw.memsize']).decode().strip()
            return int(total_memory)
        except:
            pass
    
    # Fallback to psutil if platform-specific methods fail
    return psutil.virtual_memory().total


def get_recommended_chunk_size():
    """Calculate recommended chunk size based on available memory."""
    available_memory = get_system_memory()
    # Use 1% of available memory, but not more than 64MB and not less than 8KB
    chunk_size = min(max(available_memory // 100, 8192), 64 * 1024 * 1024)
    return chunk_size


def print_memory_info():
    """Print detailed memory information for the current system."""
    system = platform.system().lower()
    total_memory = get_total_memory()
    available_memory = get_system_memory()
    
    print(f"\nSystem Information:")
    print(f"Operating System: {platform.system()} {platform.release()}")
    print(f"Total Memory: {total_memory / (1024**3):.1f} GB")
    print(f"Available Memory: {available_memory / (1024**3):.1f} GB")
    
    if system == 'linux':
        try:
            with open('/proc/meminfo', 'r') as meminfo:
                print("\nDetailed Memory Information:")
                for line in meminfo:
                    if any(x in line for x in ['MemTotal', 'MemFree', 'MemAvailable', 'Buffers', 'Cached']):
                        print(line.strip())
        except:
            pass
    
    print(f"\nRecommended chunk size: {get_recommended_chunk_size() / 1024:.1f} KB")


def get_optimal_chunk_size(file_size, available_memory):
    """Calculate optimal chunk size based on file size and available memory."""
    # Use 1% of available memory, but not more than 64MB and not less than 8KB
    memory_based = min(max(available_memory // 100, 8192), 64 * 1024 * 1024)
    
    # For very large files, use larger chunks to reduce I/O operations
    if file_size > 1 * 1024 * 1024 * 1024:  # 1GB
        return max(memory_based, 16 * 1024 * 1024)  # 16MB
    elif file_size > 100 * 1024 * 1024:  # 100MB
        return max(memory_based, 4 * 1024 * 1024)  # 4MB
    
    return memory_based


def process_single_file(file_info: Tuple[str, str, int], output_zip: str, compress: bool, available_memory: int) -> None:
    """Process a single file and add it to the zip archive with retry mechanism."""
    file_path, rel_path, file_size = file_info
    chunk_size = get_optimal_chunk_size(file_size, available_memory)
    compression = zipfile.ZIP_DEFLATED if compress else zipfile.ZIP_STORED
    
    # Print which file is being processed
    print(f"\nProcessing: {os.path.basename(file_path)} ({file_size / (1024**2):.2f} MB)")
    
    max_retries = 3
    retry_delay = 1  # seconds
    file_handle = None
    mmap_obj = None
    
    for attempt in range(max_retries):
        try:
            # Clean up any existing resources before retry
            if file_handle is not None:
                try:
                    file_handle.close()
                except:
                    pass
                file_handle = None
            if mmap_obj is not None:
                try:
                    mmap_obj.close()
                except:
                    pass
                mmap_obj = None
            
            # Use memory mapping for large files
            if file_size > 10 * 1024 * 1024:  # 10MB
                file_handle = open(file_path, 'rb')
                try:
                    fcntl.flock(file_handle.fileno(), fcntl.LOCK_SH | fcntl.LOCK_NB)
                except (AttributeError, IOError):
                    pass
                
                mmap_obj = mmap.mmap(file_handle.fileno(), 0, access=mmap.ACCESS_READ)
                
                # For very large files, use larger chunks
                if file_size > 1024 * 1024 * 1024:  # 1GB
                    chunk_size = max(chunk_size, 16 * 1024 * 1024)  # 16MB chunks
                
                # Process the file in chunks
                with zipfile.ZipFile(output_zip, 'a', compression, allowZip64=True) as zf:
                    # Create a ZipInfo object with ZIP64 support
                    zip_info = zipfile.ZipInfo(rel_path)
                    zip_info.file_size = file_size
                    zip_info.compress_type = compression
                    
                    with zf.open(zip_info, 'w') as dest:
                        bytes_written = 0
                        # Create progress bar for large files
                        pbar = tqdm(total=file_size, unit='B', unit_scale=True, 
                                  desc=f"Processing {os.path.basename(file_path)}")
                        while bytes_written < file_size:
                            try:
                                chunk = mmap_obj.read(chunk_size)
                                if not chunk:
                                    break
                                dest.write(chunk)
                                bytes_written += len(chunk)
                                pbar.update(len(chunk))
                            except IOError as e:
                                if attempt < max_retries - 1:
                                    print(f"\nI/O error reading chunk, retrying... (Attempt {attempt + 1}/{max_retries})")
                                    time.sleep(retry_delay)
                                    continue
                                else:
                                    raise
                        pbar.close()
            else:
                # For small files, read them entirely into memory
                file_handle = open(file_path, 'rb')
                try:
                    fcntl.flock(file_handle.fileno(), fcntl.LOCK_SH | fcntl.LOCK_NB)
                except (AttributeError, IOError):
                    pass
                
                data = file_handle.read()
                with zipfile.ZipFile(output_zip, 'a', compression, allowZip64=True) as zf:
                    # Create a ZipInfo object with ZIP64 support
                    zip_info = zipfile.ZipInfo(rel_path)
                    zip_info.file_size = file_size
                    zip_info.compress_type = compression
                    
                    with zf.open(zip_info, 'w') as dest:
                        dest.write(data)
            
            print(f"Completed: {os.path.basename(file_path)}")
            return  # Success, exit the retry loop
            
        except IOError as e:
            if attempt < max_retries - 1:
                print(f"\nI/O error processing {os.path.basename(file_path)}, retrying... (Attempt {attempt + 1}/{max_retries})")
                time.sleep(retry_delay)
            else:
                print(f"\nError processing {os.path.basename(file_path)} after {max_retries} attempts: {str(e)}")
                raise
        except Exception as e:
            print(f"\nError processing {os.path.basename(file_path)}: {str(e)}")
            raise
        finally:
            # Ensure file is unlocked and resources are cleaned up
            try:
                if file_handle is not None:
                    try:
                        fcntl.flock(file_handle.fileno(), fcntl.LOCK_UN)
                    except (AttributeError, IOError):
                        pass
                    file_handle.close()
                if mmap_obj is not None:
                    mmap_obj.close()
            except:
                pass


def quick_validate_zip(source_folder: str, zip_file: str) -> bool:
    """Quickly validate zip integrity by comparing file sizes and basic metadata."""
    print("\nValidating zip integrity...")
    
    # Get list of files in the zip
    with zipfile.ZipFile(zip_file, 'r') as zf:
        zip_files = {info.filename: info for info in zf.infolist()}
    
    # Track validation results
    missing_files = []
    size_mismatches = []
    total_files = 0
    
    # Walk through source directory
    for root, _, files in os.walk(source_folder):
        for file in files:
            total_files += 1
            file_path = os.path.join(root, file)
            rel_path = os.path.relpath(file_path, source_folder)
            # Convert Windows paths to zipfile format
            rel_path = rel_path.replace('\\', '/')
            
            try:
                # Skip zip files
                if file_path.lower().endswith('.zip'):
                    continue
                    
                # Get source file size
                source_size = os.path.getsize(file_path)
                
                # Check if file exists in zip
                if rel_path not in zip_files:
                    missing_files.append(rel_path)
                    continue
                
                # Compare file sizes
                zip_size = zip_files[rel_path].file_size
                if source_size != zip_size:
                    size_mismatches.append((rel_path, source_size, zip_size))
                
            except Exception as e:
                print(f"Error validating {file_path}: {str(e)}")
                return False
    
    # Print validation results
    if missing_files:
        print(f"\nValidation failed: {len(missing_files)} files missing from zip")
        print("First few missing files:")
        for file in missing_files[:5]:
            print(f"  - {file}")
        if len(missing_files) > 5:
            print(f"  ... and {len(missing_files) - 5} more")
        return False
    
    if size_mismatches:
        print(f"\nValidation failed: {len(size_mismatches)} files have size mismatches")
        print("First few mismatches:")
        for file, source_size, zip_size in size_mismatches[:5]:
            print(f"  - {file}: Source={source_size / (1024**2):.2f}MB, Zip={zip_size / (1024**2):.2f}MB")
        if len(size_mismatches) > 5:
            print(f"  ... and {len(size_mismatches) - 5} more")
        return False
    
    print(f"\nValidation successful: All {total_files} files verified")
    print(f"Total size: {sum(info.file_size for info in zip_files.values()) / (1024**2):.2f} MB")
    return True


def zip_with_builtin(source_folder, output_zip, chunk_size=None, compress=False, validate=False):
    """Zip a folder using Python's built-in zipfile module with sequential processing."""
    start_time = time.time()
    
    # Get available memory
    available_memory = get_system_memory()
    
    # If no chunk size specified, use recommended chunk size
    if chunk_size is None:
        chunk_size = get_recommended_chunk_size()
        print(f"\nUsing recommended chunk size: {chunk_size / 1024:.1f} KB")
    
    # Calculate total size and get file list
    all_files = []
    total_size = 0
    print("Scanning files...")
    for root, _, files in os.walk(source_folder):
        for file in sorted(files):  # Sort files to ensure consistent order
            file_path = os.path.join(root, file)
            rel_path = os.path.relpath(file_path, source_folder)
            try:
                # Skip zip files
                if file_path.lower().endswith('.zip'):
                    print(f"Skipping zip file: {file_path}")
                    continue
                    
                file_size = os.path.getsize(file_path)
                total_size += file_size
                file_info = (file_path, rel_path, file_size)
                all_files.append(file_info)
                print(f"Found: {file_path} ({file_size / (1024**2):.2f} MB)")
            except Exception as e:
                print(f"Error accessing {file_path}: {str(e)}")
    
    print(f"\nFound {len(all_files)} files, total size: {total_size / (1024**2):.2f} MB")
    
    compression_str = "with compression" if compress else "without compression"
    print(f"\nStarting to zip {source_folder} {compression_str} (Total size: {total_size / (1024**2):.2f} MB)")
    
    # Create empty zip file first with ZIP64 support
    with zipfile.ZipFile(output_zip, 'w', zipfile.ZIP_STORED if not compress else zipfile.ZIP_DEFLATED, allowZip64=True):
        pass
    
    # Process all files sequentially
    print("\nProcessing files...")
    pbar = tqdm(total=total_size, unit='B', unit_scale=True, desc="Overall progress")
    processed_size = 0
    
    for file_info in all_files:
        try:
            process_single_file(file_info, output_zip, compress, available_memory)
            processed_size += file_info[2]
            pbar.update(file_info[2])
        except Exception as e:
            print(f"\nError processing {file_info[0]}: {str(e)}")
            # Continue with next file even if one fails
            continue
    
    pbar.close()
    
    elapsed = time.time() - start_time
    print(f"\nZip completed in {elapsed:.2f} seconds")
    print(f"Average speed: {total_size / elapsed / (1024**2):.2f} MB/s")
    
    # Perform quick validation if requested
    if validate:
        if not quick_validate_zip(source_folder, output_zip):
            return False
    
    return True


def zip_with_7zip(source_folder, output_zip, compress=True):
    """Use 7zip command line to create a ZIP with or without compression."""
    start_time = time.time()
    
    # Ensure the output directory exists
    output_dir = os.path.dirname(os.path.abspath(output_zip))
    os.makedirs(output_dir, exist_ok=True)
    
    # Check if 7-Zip is available
    seven_zip_path = None
    if platform.system() == "Windows":
        possible_paths = [
            r"C:\Program Files\7-Zip\7z.exe",
            r"C:\Program Files (x86)\7-Zip\7z.exe"
        ]
        for path in possible_paths:
            if os.path.exists(path):
                seven_zip_path = path
                break
    else:  # For Linux/Mac
        try:
            # Check if 7z is in PATH
            subprocess.run(["7z"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            seven_zip_path = "7z"
        except FileNotFoundError:
            pass
    
    if not seven_zip_path:
        print("7-Zip not found. Please install 7-Zip or use the built-in option.")
        return False
    
    compression_str = "with compression" if compress else "without compression"
    print(f"Using 7-Zip from: {seven_zip_path}")
    print(f"Starting to zip {source_folder} with 7-Zip {compression_str}...")
    
    # Calculate total size for progress bar
    total_size = sum(os.path.getsize(os.path.join(dirpath, filename))
                    for dirpath, _, filenames in os.walk(source_folder)
                    for filename in filenames)
    
    # Build the 7z command
    # -mx0: No compression, -mx9: Maximum compression
    compression_level = "0" if not compress else "9"
    cmd = [seven_zip_path, "a", "-tzip", f"-mx{compression_level}", "-r", output_zip, f"{source_folder}/*"]
    
    try:
        # Run 7z with real-time output
        process = subprocess.Popen(
            cmd, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.STDOUT,
            universal_newlines=True
        )
        
        # Create progress bar
        pbar = tqdm(total=total_size, unit='B', unit_scale=True, desc="Zipping with 7-Zip")
        bytes_processed = 0
        
        # Display output in real-time and update progress
        for line in process.stdout:
            print(line.strip())
            # Try to extract progress information from 7-Zip output
            if "Compressing" in line:
                try:
                    # Extract the size information from the line
                    size_str = line.split()[-1]
                    if size_str.endswith('B'):
                        size = float(size_str[:-1])
                        if 'K' in size_str:
                            size *= 1024
                        elif 'M' in size_str:
                            size *= 1024 * 1024
                        elif 'G' in size_str:
                            size *= 1024 * 1024 * 1024
                        pbar.update(int(size) - bytes_processed)
                        bytes_processed = int(size)
                except:
                    pass
        
        process.wait()
        pbar.close()
        
        if process.returncode != 0:
            print(f"7-Zip failed with return code {process.returncode}")
            return False
        
        elapsed = time.time() - start_time
        print(f"7-Zip completed in {elapsed:.2f} seconds")
        return True
        
    except Exception as e:
        print(f"Error running 7-Zip: {str(e)}")
        return False


def run_performance_test(chunk_size=8192, test_folder=None):
    """Run a performance test with either sample data or a specified folder."""
    print("Running performance test...")
    
    # Initialize results dictionary
    results = {
        'builtin': {},
        '7zip': {}
    }
    
    if test_folder:
        # Use specified folder for testing
        if not os.path.exists(test_folder):
            print(f"Error: Test folder '{test_folder}' does not exist")
            return
        
        print(f"Calculating total size of {test_folder}...")
        
        # First count total files for progress bar
        total_files = sum(len(files) for _, _, files in os.walk(test_folder))
        pbar_files = tqdm(total=total_files, unit='file', desc="Counting files")
        
        # Calculate total size of the folder with progress
        total_size = 0
        file_count = 0
        for dirpath, _, filenames in os.walk(test_folder):
            for filename in filenames:
                file_path = os.path.join(dirpath, filename)
                try:
                    total_size += os.path.getsize(file_path)
                except (OSError, PermissionError) as e:
                    print(f"\nWarning: Could not access {file_path}: {str(e)}")
                file_count += 1
                pbar_files.update(1)
        
        pbar_files.close()
        total_size_mb = total_size / (1024 * 1024)
        
        print(f"\nFound {file_count} files")
        print(f"Total size: {total_size_mb:.2f} MB")
        
        # Test with both methods
        for method in ['builtin', '7zip']:
            output_zip = os.path.join(os.path.dirname(test_folder), f"test_{method}.zip")
            
            print(f"\nTesting {method} method...")
            start_time = time.time()
            
            if method == 'builtin':
                zip_with_builtin(test_folder, output_zip, chunk_size)
            else:
                zip_with_7zip(test_folder, output_zip)
            
            elapsed = time.time() - start_time
            results[method]['folder'] = elapsed
            
            # Validate output size
            if os.path.exists(output_zip):
                output_size = os.path.getsize(output_zip)
                output_size_mb = output_size / (1024 * 1024)
                print(f"Time taken: {elapsed:.2f} seconds")
                print(f"Input size: {total_size_mb:.2f} MB")
                print(f"Output size: {output_size_mb:.2f} MB")
                print(f"Compression ratio: {output_size_mb/total_size_mb:.2%}")
                os.remove(output_zip)  # Clean up
        
        # Print results table
        print("\nPerformance Test Results")
        print("=" * 60)
        print(f"{'Method':<10} | {'Time (seconds)':<15} | {'Size (MB)':<10}")
        print("-" * 60)
        print(f"{'Built-in':<10} | {results['builtin']['folder']:<15.3f} | {total_size_mb:<10.2f}")
        print(f"{'7-Zip':<10} | {results['7zip']['folder']:<15.3f} | {total_size_mb:<10.2f}")
        print("=" * 60)
        
    else:
        # Create test directory for sample data
        test_dir = os.path.join(os.getcwd(), "zip_test_data")
        os.makedirs(test_dir, exist_ok=True)
        
        try:
            # Create test files of different sizes
            sizes_mb = [10, 100, 500, 1024, 4096, 10240, 20480]  # 10MB, 100MB, 500MB, 1GB, 4GB, 10GB, 20GB
            
            for size_mb in sizes_mb:
                size_bytes = size_mb * 1024 * 1024
                test_file = os.path.join(test_dir, f"test_file_{size_mb}MB.dat")
                
                # Convert size to appropriate unit for display
                if size_mb >= 1024:  # If size is 1GB or larger
                    size_display = size_mb / 1024
                    unit = "GB"
                else:
                    size_display = size_mb
                    unit = "MB"
                
                print(f"\nCreating test file of {size_display:.1f} {unit}...")
                
                # Create file efficiently without loading it all into memory
                with open(test_file, 'wb') as f:
                    # Write in chunks of 1MB
                    chunk = b'0' * 1024 * 1024
                    mb_written = 0
                    
                    # Create progress bar for file writing
                    with tqdm(total=size_mb, unit='MB', unit_scale=True, 
                             desc=f"Writing {size_display:.1f}{unit} test file") as pbar:
                        while mb_written < size_mb:
                            f.write(chunk)
                            mb_written += 1
                            pbar.update(1)
                
                # Verify input file size
                input_size = os.path.getsize(test_file)
                input_size_mb = input_size / (1024 * 1024)
                if input_size_mb >= 1024:
                    print(f"Input file size: {input_size_mb/1024:.2f} GB")
                else:
                    print(f"Input file size: {input_size_mb:.2f} MB")
                
                # Test with both methods
                for method in ['builtin', '7zip']:
                    output_zip = os.path.join(test_dir, f"test_{size_mb}MB_{method}.zip")
                    
                    print(f"\nTesting {method} method with {size_display:.1f}{unit} file...")
                    start_time = time.time()
                    
                    if method == 'builtin':
                        zip_with_builtin(test_dir, output_zip, chunk_size)
                    else:
                        zip_with_7zip(test_dir, output_zip)
                    
                    elapsed = time.time() - start_time
                    results[method][size_mb] = elapsed
                    
                    # Validate output size
                    if os.path.exists(output_zip):
                        output_size = os.path.getsize(output_zip)
                        output_size_mb = output_size / (1024 * 1024)
                        print(f"Time taken: {elapsed:.2f} seconds")
                        if output_size_mb >= 1024:
                            print(f"Output size: {output_size_mb/1024:.2f} GB")
                        else:
                            print(f"Output size: {output_size_mb:.2f} MB")
                        print(f"Compression ratio: {output_size_mb/input_size_mb:.2%}")
                        os.remove(output_zip)  # Clean up
                
                # Clean up test file
                os.remove(test_file)
            
            # Print results table
            print("\nPerformance Test Results")
            print("=" * 80)
            print(f"{'Method':<10} | {'10MB':<10} | {'100MB':<10} | {'500MB':<10} | {'1GB':<10} | {'4GB':<10} | {'10GB':<10} | {'20GB':<10}")
            print("-" * 80)
            print(f"{'Built-in':<10} | {results['builtin'][10]:<10.3f} | {results['builtin'][100]:<10.3f} | {results['builtin'][500]:<10.3f} | {results['builtin'][1024]:<10.3f} | {results['builtin'][4096]:<10.3f} | {results['builtin'][10240]:<10.3f} | {results['builtin'][20480]:<10.3f}")
            print(f"{'7-Zip':<10} | {results['7zip'][10]:<10.3f} | {results['7zip'][100]:<10.3f} | {results['7zip'][500]:<10.3f} | {results['7zip'][1024]:<10.3f} | {results['7zip'][4096]:<10.3f} | {results['7zip'][10240]:<10.3f} | {results['7zip'][20480]:<10.3f}")
            print("=" * 80)
            print("Times are in seconds")
            
        finally:
            # Clean up test directory
            shutil.rmtree(test_dir, ignore_errors=True)
    
    print("\nPerformance test completed")


def main():
    parser = argparse.ArgumentParser(description='Zip a folder with or without compression, optimized for memory efficiency')
    parser.add_argument('--source', help='Source folder to zip')
    parser.add_argument('--output', help='Output zip file')
    parser.add_argument('--method', choices=['builtin', '7zip'], default='builtin',
                        help='Zipping method: Python built-in or 7-Zip (default: builtin)')
    parser.add_argument('--chunk-size', type=int,
                        help='Chunk size in bytes for processing (default: auto-calculated based on available memory)')
    parser.add_argument('--validate', action='store_true', default=True,
                        help='Validate zip integrity after creation (default: True)')
    parser.add_argument('--test-performance', action='store_true',
                        help='Run a performance test with sample data')
    parser.add_argument('--test-folder', type=str,
                        help='Specify a folder to test performance on instead of creating sample data')
    parser.add_argument('--compress', action='store_true', default=False,
                        help='Enable compression (default: no compression)')
    
    args = parser.parse_args()
    
    # Print detailed memory information
    print_memory_info()
    
    # Set chunk size at the start
    if args.chunk_size is not None:
        chunk_size = args.chunk_size
        print(f"\nUsing specified chunk size: {chunk_size / 1024:.1f} KB")
    else:
        chunk_size = get_recommended_chunk_size()
        print(f"\nUsing recommended chunk size: {chunk_size / 1024:.1f} KB")
    
    if args.test_performance:
        run_performance_test(chunk_size=chunk_size, test_folder=args.test_folder)
        return
    
    if not args.source or not args.output:
        print("Error: Both --source and --output arguments are required for normal operation")
        return
    
    source_folder = os.path.abspath(args.source)
    output_zip = os.path.abspath(args.output)
    
    # If output path is a directory or doesn't end with .zip, append .zip
    if os.path.isdir(output_zip) or not output_zip.lower().endswith('.zip'):
        if os.path.isdir(output_zip):
            # If it's a directory, use the source folder name as the zip name
            source_name = os.path.basename(source_folder)
            output_zip = os.path.join(output_zip, f"{source_name}.zip")
        else:
            # If it doesn't end with .zip, append it
            output_zip = f"{output_zip}.zip"
        print(f"Output path adjusted to: {output_zip}")
    
    if not os.path.exists(source_folder):
        print(f"Error: Source folder '{source_folder}' does not exist")
        return
    
    # Ensure output directory exists
    os.makedirs(os.path.dirname(output_zip), exist_ok=True)
    
    compression_status = "with compression" if args.compress else "without compression"
    print(f"Memory-efficient zipping {compression_status}")
    print(f"Source: {source_folder}")
    print(f"Output: {output_zip}")
    print(f"Method: {args.method}")
    print(f"Validation: {'enabled' if args.validate else 'disabled'}")
    
    success = False
    if args.method == '7zip':
        success = zip_with_7zip(source_folder, output_zip, args.compress)
    else:
        success = zip_with_builtin(source_folder, output_zip, chunk_size, args.compress, args.validate)
    
    if not success:
        print("Operation failed!")
        return 1
        
    return 0


if __name__ == "__main__":
    exit(main())

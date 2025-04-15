# Zip Files Without Compression

A Python script for efficiently zipping files and folders with or without compression, optimized for handling large files and parallel processing.

## Features

- Memory-efficient file processing using memory mapping for large files
- Parallel processing for small files
- Support for both built-in Python zipfile and 7-Zip methods
- Progress tracking and speed monitoring
- File locking for safe concurrent access
- Retry mechanism for handling I/O errors
- ZIP64 support for large files
- Validation of zip integrity

## Requirements

- Python 3.6+
- Required packages:
  - `tqdm` (for progress bars)
  - `psutil` (for memory management)
  - `7-Zip` (optional, for using 7-Zip method)

## Usage

```bash
python zip_files_no_compression.py [options]
```

### Options

- `--source`: Source folder to zip (required)
- `--output`: Output zip file path (required)
- `--method`: Zipping method: 'builtin' or '7zip' (default: builtin)
- `--chunk-size`: Chunk size in bytes for processing (default: auto-calculated)
- `--validate`: Validate zip integrity after creation (default: True)
- `--test-performance`: Run a performance test with sample data
- `--test-folder`: Specify a folder to test performance on
- `--compress`: Enable compression (default: no compression)

### Examples

1. Basic usage without compression:
```bash
python zip_files_no_compression.py --source /path/to/folder --output output.zip
```

2. With compression using 7-Zip:
```bash
python zip_files_no_compression.py --source /path/to/folder --output output.zip --method 7zip --compress
```

3. Run performance test:
```bash
python zip_files_no_compression.py --test-performance
```

## Performance Considerations

- The script automatically determines optimal chunk sizes based on available memory
- Large files (>1GB) are processed sequentially
- Small files are processed in parallel using ThreadPoolExecutor
- Memory mapping is used for files larger than 10MB
- File locking prevents concurrent access issues

## Error Handling

- Automatic retry mechanism for I/O errors
- File size verification
- Progress tracking with error reporting
- Validation of zip integrity

## Notes

- The script uses ZIP64 format for files larger than 4GB
- Memory usage is optimized based on available system memory
- Progress bars show both individual file progress and overall progress
- File locking is platform-dependent and may not work on all systems

## License

This script is provided as-is under the MIT License. 

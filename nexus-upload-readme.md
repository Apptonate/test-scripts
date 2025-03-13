# Nexus Repository Upload Script

A memory-efficient Python script for zipping local directories and uploading them to a Nexus Repository.

## Features

- 📦 Creates zip archives of local file directories
- 🚀 Streams uploads to Nexus Repository
- 🧠 Memory-efficient processing for large file sets
- ⏱️ Progress tracking with ETA and speed information
- 🔄 Automatic retry on connection issues
- 🖥️ Works with both Windows and Linux environments

## Requirements

- Python 3.6+
- Nexus Repository instance with appropriate permissions
- Python packages:
  - `requests`
  - `tqdm` (optional, for progress bars)

## Installation

1. Clone or download this repository
2. Install required dependencies:

```bash
pip install requests
pip install tqdm  # Optional, for progress bars
```

## Usage

```bash
python nexus-upload-script.py --source /path/to/source/directory \
                             --nexus-url http://your-nexus-instance:8081 \
                             --repository your-repository-name \
                             --username admin \
                             --password your-password
```

### Required Arguments

- `--source`: Directory containing files to zip
- `--nexus-url`: Base URL of the Nexus repository
- `--repository`: Name of the repository

### Optional Arguments

- `--username`: Nexus username (can also use NEXUS_USERNAME environment variable)
- `--password`: Nexus password (can also use NEXUS_PASSWORD environment variable)
- `--directory`: Target directory in Nexus repository
- `--zip-file`: Custom name for the ZIP file (default: timestamp-based)
- `--keep-zip`: Keep the ZIP file after upload (default: remove after successful upload)
- `--chunk-size`: Chunk size for file operations in bytes (default: 1MB)
- `--max-memory`: Maximum memory usage in MB (default: 100MB)
- `--temp-dir`: Custom temporary directory for processing large files

## Environment Variables

The script supports the following environment variables:

- `NEXUS_USERNAME`: Nexus repository username
- `NEXUS_PASSWORD`: Nexus repository password

## Troubleshooting

### Uploads Timing Out

If uploads are timing out, try:

1. Reducing `--chunk-size` to a smaller value (e.g., 1MB or 512KB)
2. Ensuring Nexus server has sufficient resources
3. Checking network stability between client and server

### Docker Container Issues

When running Nexus in a Docker container:

1. Ensure your container has sufficient memory and CPU resources
2. Check Docker logs for any error messages
3. Consider adjusting network settings if uploads are slow

### Nexus Configuration

Ensure these settings are properly configured in your Nexus server:

```properties
# Increase maximum request size
nexus.http.request.maximumSize=11500MB
# Keep files during upload
nexus.uploadComponent.keepFiles=true
# Increase request timeouts
nexus.http.request.idleTimeout=600000
nexus.http.request.timeout=600000
# Jetty configuration
jetty.max.form.content.size=11500MB
jetty.request.header.size=32768
```

## Examples

### Basic Usage

```bash
python nexus-upload-script.py --source ./my_files --nexus-url http://localhost:8081 --repository my-repo --username admin --password admin123
```

### Specify Upload Directory and Keep ZIP

```bash
python nexus-upload-script.py --source ./my_files --nexus-url http://localhost:8081 --repository my-repo --directory path/to/store --keep-zip --username admin --password admin123
```

### Large File Processing with Custom Settings

```bash
python nexus-upload-script.py --source ./large_files --nexus-url http://localhost:8081 --repository my-repo --chunk-size 524288 --max-memory 500 --temp-dir /tmp/zip_processing --username admin --password admin123
```

## License

[MIT License](LICENSE)

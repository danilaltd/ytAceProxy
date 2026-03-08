# Proxy ACE

A Python-based media streaming proxy service that handles multiple channel streams and redirects YouTube/streaming URLs to direct playback links.

## Overview

Proxy ACE is an asynchronous HTTP proxy server built with `aiohttp` that:

1. **Streams IPTV channels** - Provides HTTP streaming access to IPTV channels via `/ace/{channel}` endpoint
2. **Resolves streaming URLs** - Uses `yt-dlp` to extract direct playable URLs from YouTube and other streaming services
3. **Manages multiple clients** - Handles concurrent client connections with queue-based buffering
4. **Hot-reloading configuration** - Watches `channels.json` for changes and syncs automatically
5. **Reconnection logic** - Implements exponential backoff retry strategy for upstream connection failures

## Create and activate virtual environment

Create a virtual environment:

```bash
python3 -m venv venv
```

Activate it:

```bash
source venv/bin/activate
```

Install dependencies:

```bash
pip install -r requirements.txt
```

Check installed packages:

```bash
pip list
```

## Configuration

### channels.json Format

```json
{
  "ace": {
    "channel_name": "stream_id_or_hash"
  },
  "yt-dlp": {
    "channel_name": "https://url.to.stream"
  }
}
```

- **ace** section: IPTV channel stream IDs (processed through ACE proxy)
- **yt-dlp** section: Direct URLs to streaming services (YouTube, X/Twitter, etc.)

## Running the Application

```bash
python -m proxy_ace.proxy_ace
```

Server listens on port `8081` (configurable in proxy_ace.py)

## Set up as systemd service

To run this project as a systemd service, create a unit file:

```ini
sudo nano /etc/systemd/system/proxy-ace.service
```

Paste the following:

```ini
[Unit]
Description=My Python Stream Script
After=network.target

[Service]
Type=simple
GuessMainPID=no
WorkingDirectory=/path/to/project       
ExecStart=/path/to/project/venv/bin/python -m proxy_ace.proxy_ace
Restart=always
User=daniil
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

Then enable and start the service:

```bash
sudo systemctl daemon-reload
sudo systemctl enable proxy-ace
sudo systemctl start proxy-ace
```

Check logs:

```bash
journalctl -u proxy-ace -f
```

## Architecture

### Core Components

- **proxy_ace.py** - Main application server with signal handling and file watcher for config changes
- **handler.py** - HTTP request handlers for client streaming and URL redirection
- **producer.py** - Upstream connection manager that fetches streams and distributes to clients
- **config.py** - Configuration loader and `yt-dlp` integration for URL extraction
- **models.py** - Data models for channels, clients, and redirects
- **channels.json** - Configuration file with channel definitions

### Data Models

- `Channel` - Contains stream URL, connected clients, producer task, and synchronization primitives
- `RedirectChannel` - Maps original URL to extracted playable URL for streaming services
- `Client` - Represents a connected client with response stream and data queue

## API Endpoints

### ACE Stream Endpoint
```
GET /ace/{channel}
```
Returns HLS/MPEG-TS stream for the specified channel. Fails with 404 if channel not found.

### YT-DLP Redirect Endpoint
```
GET /yt_dlp/{channel}
```
Returns 302 redirect to the direct playable URL extracted by `yt-dlp`. Falls back to placeholder channel if channel not found.

## Dependencies

Key dependencies:
- `aiohttp` - Async HTTP server and client
- `yt-dlp` - YouTube and streaming service URL extraction
- `watchdog` - File system monitoring for config changes
- `asyncio` - Async/await concurrency

## Features

### Concurrent Client Handling
- Queue-based buffering (max 25,000 items per client)
- Auto-drops slow clients when queue fills
- Proper cleanup on disconnection

### Stream Management
- Automatic producer task creation per channel
- Stops producer when no active clients
- Handles upstream reconnection with exponential backoff
- Timeout handling with configurable delays

### Configuration Management
- Automatic config sync on file changes (debounced)
- Full sync every hour
- Graceful handling of missing channels
- Supports adding/removing channels without restart

### Error Handling
- Comprehensive logging for debugging
- Automatic client disconnection on errors
- Producer retry logic (up to 3 attempts with exponential backoff)
- Signal handling for graceful shutdown (SIGTERM, SIGINT)

## Monitoring

The application logs:
- Client connections/disconnections
- Stream status (started, finished, errors)
- Queue overflow events
- URL extraction progress
- Configuration sync events

## Performance Considerations

1. **Queue Management** - Slow clients are dropped to prevent memory buildup
2. **Process Pooling** - `yt-dlp` runs in separate processes to avoid blocking
3. **Async I/O** - All network operations are non-blocking
4. **Resource Cleanup** - Proper cleanup of tasks, processes, and connections
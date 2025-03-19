#!/usr/bin/env python3

import asyncio
import ssl
import random
import string
import time
import h2.connection
import h2.events
import h2.config
# import aiohttp
# import socket
import logging
import colorlog
import h2.settings
from urllib.parse import urlparse
import sys
import getopt

import os
from logging.handlers import RotatingFileHandler

def add_file_logging(log, log_file=None, max_size_mb=10, backup_count=3):
    """
    Set up optional file logging.
    
    Args:
        log_file (str, optional): Path to the log file. If None, file logging is disabled.
        max_size_mb (int): Maximum size of log file in MB before rotation.
        backup_count (int): Number of backup files to keep.
    
    Returns:
        logging.Logger: The configured logger instance.
    """
    if log_file:
        log_dir = os.path.dirname(log_file)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir)
            
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=max_size_mb * 1024 * 1024,
            backupCount=backup_count
        )
        
        file_formatter = logging.Formatter(
            '%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(file_formatter)
        
        log.addHandler(file_handler)
        
        log.info(f"File logging enabled: {log_file}")
    
    return log

level_map = {
    'DEBUG': 'D',
    'INFO': 'I',
    'WARNING': 'W',
    'ERROR': 'E',
    'CRITICAL': 'C'
}

class SingleCharLevelNameFormatter(colorlog.ColoredFormatter):
    def format(self, record):
        record.lname = level_map.get(record.levelname, record.levelname)
        return super().format(record)

handler = colorlog.StreamHandler()
handler.setFormatter(SingleCharLevelNameFormatter(
    '%(log_color)s%(asctime)s.%(msecs)03d [%(lname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    log_colors={
        'DEBUG': 'cyan',
        'INFO': 'green',
        'WARNING': 'yellow',
        'ERROR': 'red',
        'CRITICAL': 'red,bg_white',
    }
))
logging.basicConfig(
    level=logging.INFO, handlers=[handler]
)
log = logging.getLogger('h2tester')

class HTTP2Connection:
    def __init__(self, uri, method="GET", body=None, headers=None, ping_interval=1, request_interval=7):
        parsed_uri = urlparse(uri)
        self.req = {
            'uri': uri,
            'method': method,
            'scheme': parsed_uri.scheme or 'https',
            'host': parsed_uri.netloc.split(':')[0],
            'port': int(parsed_uri.netloc.split(':')[1]) if ':' in parsed_uri.netloc else 443,
            'path': parsed_uri.path or '/',
            'query': parsed_uri.query,
            'body': body,
            'headers': headers or {}
        }
        
        if self.req['query']:
            self.req['path'] = f"{self.req['path']}?{self.req['query']}"
            
        self.host = self.req['host']
        self.port = self.req['port']
        self.ping_interval = ping_interval
        self.request_interval = request_interval
        self.request_active = False
        self.reader = None
        self.writer = None
        self.conn = None
        
        self.current = None
        self.gen = 0
        self.stream = {}

    async def connect(self):
        """Establish a new HTTP/2 connection."""
        ssl_context = ssl.create_default_context()
        ssl_context.set_alpn_protocols(["h2"])

        while True:
            try:
                self.gen += 1
                self.current = asyncio.Event()

                start_time = time.time()
                log.info(f"Connecting to {self.host}:{self.port} via HTTP/2 (gen {self.gen})")
                self.reader, self.writer = await asyncio.open_connection(self.host, self.port, ssl=ssl_context)
                log.info(f"Connected  to {self.host}:{self.port} in {time.time() - start_time:.3f}s")

                self.conn = h2.connection.H2Connection(config=h2.config.H2Configuration(client_side=True))
                self.conn.initiate_connection()

                asyncio.create_task(self.ping_loop())
                asyncio.create_task(self.request_loop())

                try:
                    while True:
                        data = await self.reader.read(4096)
                        if not data:
                            raise Exception(f"Connection lost.")
                            break

                        events = self.conn.receive_data(data)
                        for event in events:
                            # print(event)
                            if hasattr(event, "stream_id") and self.stream.get(event.stream_id):
                                await self.stream[event.stream_id].put(event)
                            elif isinstance(event, h2.events.StreamEnded) and self.stream.get(event.stream_id):
                                log.error(f"Unexpected {event}")
                                del self.stream[event.stream_id]
                            elif isinstance(event, h2.events.DataReceived):
                                log.error(f"Unexpected {event}")
                                self.conn.acknowledge_received_data(event.flow_controlled_length, event.stream_id)
                            elif isinstance(event, h2.events.PingAckReceived):
                                self.pong.put_nowait(event.ping_data)
                            elif isinstance(event, h2.events.RemoteSettingsChanged):
                                chg = {}
                                for s in h2.settings.SettingCodes:
                                    if s.value in event.changed_settings:
                                        chg[s.name] = (event.changed_settings[s.value].original_value, event.changed_settings[s.value].new_value)
                                log.info(f"Remote settings changed: {chg}")
                            elif isinstance(event, h2.events.SettingsAcknowledged):
                                pass
                            elif isinstance(event, h2.events.WindowUpdated):
                                pass
                            elif isinstance(event, h2.events.ConnectionTerminated):
                                raise Exception(f"Connection terminated by server: {event.error_code}")
                            else:
                                log.warning(f"Unhandled event: {event}")
                        
                        data = self.conn.data_to_send()
                        if data:
                            self.writer.write(data)
                            await self.writer.drain()
                except Exception as e:
                    if self.request_active:
                        log.error(f"[!] Error reading data while request is active: {e}")
                        self.request_active = False
                    else:
                        log.warning(f"Connection closed: {e}")
                finally:
                    log.warning(f"Notifying all waiters")
                    self.current.set()

            except KeyboardInterrupt:
                print("Finishing...")
                await self.close()
                break
            except Exception as e:
                log.error(f"[!] {e} at line {e.__traceback__.tb_lineno}")
                await asyncio.sleep(1)

    async def ping_loop(self):
        """Periodically send PING frames to check connection health."""
        gen = self.gen
        self.pong = asyncio.Queue(maxsize=1)
        while gen == self.gen:
            try:
                start_time = time.time()
                ping_data = self.generate_ping_data()
                self.conn.ping(ping_data)
                self.writer.write(self.conn.data_to_send())
                await self.writer.drain()
                pong_data = await self.pong.get()
                self.pong.task_done()
                if ping_data == pong_data:
                    log.info(f"PONG received in {time.time() - start_time:.3f}s")
                else:
                    log.error(f"PONG received {ping_data} ≠ {pong_data} in {time.time() - start_time:.3f}s")
                await asyncio.wait_for(self.current.wait(), timeout=self.ping_interval - (time.time() - start_time))
            except asyncio.TimeoutError:
                pass
            except Exception as e:
                print(f"[!] PING failed: {e}")
                break
        log.warning(f"PING loop finished (gen {self.gen})")
    
    async def request_loop(self):
        """Periodically send requests"""
        gen = self.gen
        while gen == self.gen:
            try:
                start_time = time.time()
                stream_id = self.conn.get_next_available_stream_id()
                self.stream[stream_id] = asyncio.Queue(maxsize=1)

                headers = [
                    (":method", self.req['method']),
                    (":authority", self.host),
                    (":scheme", self.req['scheme']),
                    (":path", self.req['path']),
                ]
                
                for name, value in self.req['headers'].items():
                    headers.append((name.lower(), value))
                end_stream = self.req['body'] is None
                self.conn.send_headers(stream_id, headers, end_stream=end_stream)
                
                if not end_stream:
                    self.conn.send_data(stream_id, self.req['body'], end_stream=True)
                
                self.request_active = True
                self.writer.write(self.conn.data_to_send())
                await self.writer.drain()
                
                headers = {}
                data = b''

                while True:
                    event = await self.stream[stream_id].get()
                    if isinstance(event, h2.events.StreamEnded):
                        del self.stream[stream_id]
                        break
                    elif isinstance(event, h2.events.ResponseReceived):
                        for header in event.headers:
                            k = str(header[0],'ascii')
                            v = str(header[1],'ascii')
                            if k in headers:
                                headers[k] += f", {v}"
                            else:
                                headers[k] = v
                    elif isinstance(event, h2.events.DataReceived):
                        data = data + event.data
                        self.conn.acknowledge_received_data(event.flow_controlled_length, event.stream_id)
                self.request_active = False
                status = int(headers[':status'])

                if status < 400:
                    log.info(f"{self.req['method']} {self.req['path']} → {status} +{len(data)} in {time.time() - start_time:.3f}s")
                elif status < 500:
                    log.warning(f"{self.req['method']} {self.req['path']} → {status} +{len(data)} in {time.time() - start_time:.3f}s")
                else:
                    log.error(f"{self.req['method']} {self.req['path']} → {status} +{len(data)} in {time.time() - start_time:.3f}s")
                
                await asyncio.wait_for(self.current.wait(), timeout=self.request_interval - (time.time() - start_time))
            except asyncio.TimeoutError:
                pass
            except Exception as e:
                log.error(f"[!] XXX: {e}")
                raise e
                self.request_active = False
                break
        log.warning(f"Request loop finished (gen {self.gen})")
    
    @staticmethod
    def generate_ping_data():
        return bytes(random.choices(string.ascii_letters.encode(), k=8))

    async def close(self):
        if self.writer:
            self.conn.close_connection()
            self.writer.write(self.conn.data_to_send())
            await self.writer.drain()
            self.writer.close()
            await self.writer.wait_closed()
            print("[+] Connection closed.")

usage = """
HTTP/2 Tester - Test HTTP/2 connections and requests

Usage: python h2tester.py [options] [url]

Options:
  -h, --help              Show this help message and exit
  -u, --url=URL           Target URL (default: https://nghttp2.org/httpbin/get)
  -m, --method=METHOD     HTTP method (default: GET)
  -d, --data=DATA         Request body data
  -H, --header=HEADER     Request header in format "Name: Value" (can be used multiple times)
  -i, --interval=SECONDS  Request interval in seconds (default: 7)
  -p, --ping=SECONDS      Ping interval in seconds (default: 1)
  -l, --log=FILE          Log to specified file
  -L, --log-auto          Enable automatic logging (generates filename based on uri)
  -v, --verbose           Increase verbosity (debug mode)
  -q, --quiet             Suppress console output (quiet mode)
"""


async def main():
    # Default values
    uri = "https://nghttp2.org/httpbin/get"
    method = "GET"
    body = None
    headers = {}
    log_file = None
    smart_log = False
    ping_interval = 1
    request_interval = 7
    log_level = logging.INFO
    
    # Command line arguments
    
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hu:m:d:H:i:p:l:Lvq", 
                                   ["help", "url=", "method=", "data=", "header=", 
                                    "interval=", "ping=", "log=", "log-auto", "verbose", "quiet"])
    except getopt.GetoptError as err:
        print(str(err))
        print(usage)
        sys.exit(2)
    
    for opt, arg in opts:
        if opt in ("-h", "--help"):
            print(usage)
            sys.exit()
        elif opt in ("-u", "--url"):
            uri = arg
        elif opt in ("-m", "--method"):
            method = arg.upper()
        elif opt in ("-d", "--data"):
            body = arg.encode()
        elif opt in ("-H", "--header"):
            if ":" in arg:
                name, value = arg.split(":", 1)
                headers[name.strip()] = value.strip()
            else:
                print(f"Invalid header format: {arg}. Should be 'Name: Value'")
        elif opt in ("-i", "--interval"):
            try:
                request_interval = float(arg)
            except ValueError:
                print(f"Invalid interval value: {arg}. Using default: {request_interval}")
        elif opt in ("-p", "--ping"):
            try:
                ping_interval = float(arg)
            except ValueError:
                print(f"Invalid ping interval value: {arg}. Using default: {ping_interval}")
        elif opt in ("-l", "--log"):
            log_file = arg
            smart_log = False  # Explicit log file overrides smart logging
        elif opt in ("-L", "--log-auto"):
            smart_log = True
            log_file = None  # Will be set later
        elif opt in ("-v", "--verbose"):
            log_level = logging.DEBUG
            log.setLevel(logging.DEBUG)
        elif opt in ("-q", "--quiet"):
            # Remove console handler
            for handler in log.handlers[:]:
                if isinstance(handler, logging.StreamHandler):
                    log.removeHandler(handler)
    
    # Use the first non-option argument as URL if provided
    if args:
        uri = args[0]
    
    # Create a temporary tester instance to parse the URL
    temp_tester = HTTP2Connection(uri=uri, method=method)
    
    # Generate smart log filename if smart logging is enabled
    if smart_log:
        # Format the current time
        timestamp = time.strftime("%Y%m%dT%H%M%S")
        
        # Sanitize path for filename (replace / with -)
        safe_path = temp_tester.req['path'].replace('/', '-')
        if safe_path.startswith('-'):
            safe_path = safe_path[1:]
        if not safe_path:
            safe_path = "root"
            
        # Keep path to reasonable length
        if len(safe_path) > 30:
            safe_path = safe_path[:30]
            
        # Generate filename: h2t-<method>-<host>-<path>-<datetime>.log
        log_file = f"h2t-{method.lower()}-{temp_tester.host}-{safe_path}-{timestamp}.log"
    
    # Configure file logging if a log file was specified or generated
    if log_file:
        add_file_logging(log, log_file)
    
    # Create the actual tester with all parameters
    tester = HTTP2Connection(
        uri=uri,
        method=method,
        body=body,
        headers=headers,
        ping_interval=ping_interval,
        request_interval=request_interval
    )
    
    log.info(f"Starting HTTP/2 tester for {tester.req['uri']}")
    log.info(f"Method: {method}, Ping interval: {ping_interval}s, Request interval: {request_interval}s")
    
    await tester.connect()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Keyboard interrupt")

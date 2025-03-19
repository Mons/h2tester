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
import re

import os
from logging.handlers import RotatingFileHandler

# Global time formatting configuration
# TIME_FORMAT = ".3s"  # Default format: 3 decimal places, seconds
TIME_PRECISION = 3
TIME_UNIT = 's'
TIME_UNIT_MULTIPLIER = 1
TIME_FORMATS = {
    's': '.3s',
    'm': '.0m',
    'u': '.0u',
}

# Default timeout values
DEFAULT_CONNECT_TIMEOUT = 5.0  # Connection timeout in seconds
DEFAULT_RESPONSE_TIMEOUT = 10.0  # Response timeout in seconds
DEFAULT_PONG_TIMEOUT = 2.0  # PING response timeout in seconds
DEFAULT_IO_TIMEOUT = max(DEFAULT_RESPONSE_TIMEOUT, DEFAULT_PONG_TIMEOUT)  # IO operation timeout

def parse_time_format(format_str):
    if format_str in TIME_FORMATS:
        format_str =  TIME_FORMATS[format_str]
    match = re.match(r'0?\.(\d+)([a-zµ]+)', format_str)
    if not match:
        log.warning(f"Invalid time format: {format_str}, using default (.3s)")
        return 3, 's', 1
    
    precision = int(match.group(1))
    unit = match.group(2)
    unit_multiplier = 1
    # Normalize unit
    if unit in ['m', 'ms']:
        unit = 'ms'
        unit_multiplier = 1000
    elif unit in ['u', 'µ', 'us', 'µs']:
        unit = 'µs'
        unit_multiplier = 1000000
    elif unit == 's':
        unit = 's'
    else:
        log.warning(f"Unknown time unit: {unit}, using seconds")
        unit = 's'
    
    return precision, unit, unit_multiplier

def time_format(interval, format_str=None):
    if TIME_PRECISION == 0:
        return f"{int(interval * TIME_UNIT_MULTIPLIER)}{TIME_UNIT}"
    else:
        format_spec = f"{{:.{TIME_PRECISION}f}}{TIME_UNIT}"
        return format_spec.format(interval * TIME_UNIT_MULTIPLIER)

def since(start_time, format_str=None):
    return time_format(time.time() - start_time, format_str)

def add_file_logging(log, log_file=None, max_size_mb=10, backup_count=3):
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
    def __init__(self, uri, method="GET", body=None, headers=None, ping_interval=1, request_interval=7,
                 connect_timeout=DEFAULT_CONNECT_TIMEOUT, 
                 response_timeout=DEFAULT_RESPONSE_TIMEOUT,
                 pong_timeout=DEFAULT_PONG_TIMEOUT,
                 io_timeout=None):
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
        
        # Timeout settings
        self.connect_timeout = connect_timeout
        self.response_timeout = response_timeout
        self.pong_timeout = pong_timeout
        # If io_timeout is not provided, use the maximum of response and pong timeouts
        self.io_timeout = io_timeout if io_timeout is not None else max(response_timeout, pong_timeout)
        
        self.current = None
        self.gen = 0
        self.stream = {}
    
    async def connreset(self):
        if self.conn:
            self.conn.close_connection()
            self.conn = None
        if self.writer:
            self.writer.close()
            self.writer = None
        if self.reader:
            self.reader = None

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
                
                # Add connection timeout
                try:
                    self.reader, self.writer = await asyncio.wait_for(
                        asyncio.open_connection(self.host, self.port, ssl=ssl_context),
                        timeout=self.connect_timeout
                    )
                    peername = self.writer.get_extra_info('peername')
                    remote_ip = None
                    if peername:
                        remote_ip, _ = peername
                        
                    log.info(f"Connected to {self.host}:{self.port} ({remote_ip}) in {since(start_time)}")
                except asyncio.TimeoutError:
                    log.error(f"Connection timeout after {since(start_time)}")
                    continue
                

                self.conn = h2.connection.H2Connection(config=h2.config.H2Configuration(client_side=True))
                self.conn.initiate_connection()

                # Send initial data with timeout
                initial_data = self.conn.data_to_send()
                if initial_data:
                    try:
                        self.writer.write(initial_data)
                        await asyncio.wait_for(self.writer.drain(), timeout=self.io_timeout)
                        log.debug(f"Initial connection data sent")
                    except asyncio.TimeoutError:
                        log.error(f"IO timeout while sending initial data")
                        continue
                
                asyncio.create_task(self.ping_loop())
                asyncio.create_task(self.request_loop())

                try:
                    while True:
                        start_read = time.time()
                        try:
                            # Add timeout for reading data
                            data = await asyncio.wait_for(
                                self.reader.read(4096),
                                timeout=self.io_timeout
                            )
                        except asyncio.TimeoutError:
                            log.error(f"IO timeout after {since(start_read)}, reconnecting")
                            break
                            
                        if not data:
                            raise Exception(f"Connection lost")

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
                            try:
                                self.writer.write(data)
                                await asyncio.wait_for(self.writer.drain(), timeout=self.io_timeout)
                            except asyncio.TimeoutError:
                                log.error(f"IO timeout during write operation")
                                break
                except Exception as e:
                    if self.request_active:
                        log.error(f"[!] Error reading data while request is active: {e}")
                        self.request_active = False
                    else:
                        log.warning(f"Connection closed: {e}")
                finally:
                    log.warning(f"Notifying all waiters")
                    self.current.set()
                    # Close the writer if it exists
                    if self.writer:
                        try:
                            self.writer.close()
                            await self.writer.wait_closed()
                        except Exception as e:
                            log.debug(f"Error closing writer: {e}")

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
        current = self.current
        self.pong = asyncio.Queue(maxsize=1)
        while gen == self.gen:
            try:
                start_time = time.time()
                ping_data = self.generate_ping_data()
                self.conn.ping(ping_data)
                
                # Add IO timeout for write operations
                try:
                    self.writer.write(self.conn.data_to_send())
                    await asyncio.wait_for(self.writer.drain(), timeout=self.io_timeout)
                except asyncio.TimeoutError:
                    log.error(f"IO timeout while sending PING")
                    current.set()
                    break
                
                # Add timeout for waiting for PONG
                try:
                    pong_data = await asyncio.wait_for(
                        self.pong.get(),
                        timeout=self.pong_timeout
                    )
                    self.pong.task_done()
                    
                    if ping_data == pong_data:
                        log.info(f"PONG received in {since(start_time)}")
                    else:
                        log.error(f"PONG received {ping_data} ≠ {pong_data} in {since(start_time)}")
                except asyncio.TimeoutError:
                    log.error(f"PONG timeout after {time_format(self.pong_timeout)}, reconnecting")
                    # Set the current event to force termination
                    await self.connreset()
                    break
                    
                await asyncio.wait_for(current.wait(), timeout=self.ping_interval - (time.time() - start_time))
            except asyncio.TimeoutError:
                pass
            except Exception as e:
                log.error(f"[!] PING failed: {e}")
                break
        log.warning(f"PING loop finished (gen {self.gen})")
    
    async def request_loop(self):
        """Periodically send requests"""
        gen = self.gen
        current = self.current
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
                
                # Add IO timeout for write operations
                try:
                    self.writer.write(self.conn.data_to_send())
                    await asyncio.wait_for(self.writer.drain(), timeout=self.io_timeout)
                except asyncio.TimeoutError:
                    log.error(f"IO timeout while sending request")
                    current.set()
                    break
                
                headers = {}
                data = b''
                
                # Add timeout for complete request-response cycle
                request_complete = False
                
                try:
                    async with asyncio.timeout(self.response_timeout):
                        while True:
                            event = await self.stream[stream_id].get()
                            if isinstance(event, h2.events.StreamEnded):
                                del self.stream[stream_id]
                                request_complete = True
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
                                # Add IO timeout for acknowledgment
                                try:
                                    self.conn.acknowledge_received_data(event.flow_controlled_length, event.stream_id)
                                    out_data = self.conn.data_to_send()
                                    if out_data:
                                        self.writer.write(out_data)
                                        await asyncio.wait_for(self.writer.drain(), timeout=self.io_timeout)
                                except asyncio.TimeoutError:
                                    log.error(f"IO timeout while acknowledging data")
                                    raise
                except asyncio.TimeoutError:
                    log.error(f"Response timeout after {time_format(self.response_timeout)}, reconnecting")
                    # Clean up the stream entry if it exists
                    if stream_id in self.stream:
                        del self.stream[stream_id]
                    # Set the current event to force termination
                    break
                
                self.request_active = False
                
                if request_complete and ':status' in headers:
                    status = int(headers[':status'])
                    if status < 400:
                        log.info(f"{self.req['method']} {self.req['path']} → {status} +{len(data)} in {since(start_time)}")
                    elif status < 500:
                        log.warning(f"{self.req['method']} {self.req['path']} → {status} +{len(data)} in {since(start_time)}")
                    else:
                        log.error(f"{self.req['method']} {self.req['path']} → {status} +{len(data)} in {since(start_time)}")
                
                await asyncio.wait_for(current.wait(), timeout=self.request_interval - (time.time() - start_time))
            except asyncio.TimeoutError:
                pass
            except Exception as e:
                log.error(f"[!] Request error: {e}")
                # Don't raise the exception
                self.request_active = False
                # Set the current event to force termination
                await self.connreset()
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

DEFAULT_PING_INTERVAL = 1.0
DEFAULT_REQUEST_INTERVAL = 3.0
DEFAULT_URL = "https://nghttp2.org/httpbin/get"
DEFAULT_METHOD = "GET"

usage = f"""
HTTP/2 Tester - Test HTTP/2 connections and requests

Usage: python h2tester.py [options] [url]

Options:
  -h, --help              Show this help message and exit
  -u, --url=URL           Target URL (default: {DEFAULT_URL})
  -m, --method=METHOD     HTTP method (default: {DEFAULT_METHOD})
  -d, --data=DATA         Request body data
  -H, --header=HEADER     Request header in format "Name: Value" (can be used multiple times)
  -i, --interval=SECONDS  Request interval in seconds (default: {DEFAULT_REQUEST_INTERVAL})
  -p, --ping=SECONDS      Ping interval in seconds (default: {DEFAULT_PING_INTERVAL})
  -l, --log=FILE          Log to specified file
  -L, --log-auto          Enable automatic logging (generates filename based on uri)
  -T, --time-format=FMT   Time format (.3s, .1m, .0µs, .0u - precision and unit. Presets: s, m, u)
  
  # Timeout options
  --connect-timeout=SEC   Connection establishment timeout (default: {DEFAULT_CONNECT_TIMEOUT}s)
  --response-timeout=SEC  Response wait timeout (default: {DEFAULT_RESPONSE_TIMEOUT}s)
  --pong-timeout=SEC      PING response timeout (default: {DEFAULT_PONG_TIMEOUT}s)
  --io-timeout=SEC        IO operation timeout (default: max of response/pong timeout)
  
  -v, --verbose           Increase verbosity (debug mode)
  -q, --quiet             Suppress console output (quiet mode)
"""

async def main():
    # Default values
    uri = DEFAULT_URL
    method = DEFAULT_METHOD
    body = None
    headers = {}
    log_file = None
    smart_log = False
    ping_interval = DEFAULT_PING_INTERVAL
    request_interval = DEFAULT_REQUEST_INTERVAL
    connect_timeout = DEFAULT_CONNECT_TIMEOUT
    response_timeout = DEFAULT_RESPONSE_TIMEOUT
    pong_timeout = DEFAULT_PONG_TIMEOUT
    io_timeout = None  # Will be calculated from response and pong timeouts if not specified
    global TIME_PRECISION, TIME_UNIT, TIME_UNIT_MULTIPLIER
    
    # Command line arguments
    
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hu:m:d:H:i:p:l:LT:vq", 
                                   ["help", "url=", "method=", "data=", "header=", 
                                    "interval=", "ping=", "log=", "log-auto", 
                                    "time-format=", "verbose", "quiet",
                                    "connect-timeout=", "response-timeout=", 
                                    "pong-timeout=", "io-timeout="])
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
        elif opt in ("-L", "--log-auto"):
            smart_log = True
        elif opt in ("-T", "--time-format"):
            TIME_PRECISION, TIME_UNIT, TIME_UNIT_MULTIPLIER = parse_time_format(arg)
        elif opt == "--connect-timeout":
            try:
                connect_timeout = float(arg)
            except ValueError:
                print(f"Invalid connect timeout: {arg}. Using default: {connect_timeout}")
        elif opt == "--response-timeout":
            try:
                response_timeout = float(arg)
            except ValueError:
                print(f"Invalid response timeout: {arg}. Using default: {response_timeout}")
        elif opt == "--pong-timeout":
            try:
                pong_timeout = float(arg)
            except ValueError:
                print(f"Invalid pong timeout: {arg}. Using default: {pong_timeout}")
        elif opt == "--io-timeout":
            try:
                io_timeout = float(arg)
            except ValueError:
                print(f"Invalid IO timeout: {arg}. Using calculated default.")
        elif opt in ("-v", "--verbose"):
            log.setLevel(logging.DEBUG)
        elif opt in ("-q", "--quiet"):
            # Remove console handler
            for handler in log.handlers[:]:
                if isinstance(handler, logging.StreamHandler):
                    log.removeHandler(handler)
    
    if args:
        uri = args[0]
    
    # Generate smart log filename if smart logging is enabled
    if smart_log:
        if log_file:
            raise ValueError("Cannot specify both --log and --log-auto")

        parsed_uri = urlparse(uri)
        host = parsed_uri.netloc.split(':')[0]
        path = parsed_uri.path or '/'

        safe_path = path.replace('/', '-')
        if safe_path.startswith('-'):
            safe_path = safe_path[1:]
        if len(safe_path) > 30:
            safe_path = safe_path[:30]

        timestamp = time.strftime("%Y%m%dT%H%M%S")

        log_file = f"h2t-{method.lower()}-{host}-{safe_path}-{timestamp}.log"
    
    if log_file:
        add_file_logging(log, log_file)
    
    # If io_timeout is not explicitly set, calculate it
    if io_timeout is None:
        io_timeout = max(response_timeout, pong_timeout)
    
    tester = HTTP2Connection(
        uri=uri,
        method=method,
        body=body,
        headers=headers,
        ping_interval=ping_interval,
        request_interval=request_interval,
        connect_timeout=connect_timeout,
        response_timeout=response_timeout,
        pong_timeout=pong_timeout,
        io_timeout=io_timeout
    )
    
    log.info(f"Starting HTTP/2 tester for {method} {tester.req['uri']}")
    log.info(f"Ping interval: {time_format(ping_interval)}, Request interval: {time_format(request_interval)}")
    log.info(f"Timeouts: connect={time_format(connect_timeout)}, response={time_format(response_timeout)}, pong={time_format(pong_timeout)}, io={time_format(io_timeout)}")
    
    await tester.connect()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Keyboard interrupt")

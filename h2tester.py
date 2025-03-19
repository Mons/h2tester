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
        # Create directory for log file if it doesn't exist
        log_dir = os.path.dirname(log_file)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir)
            
        # Create a file handler for logging to a file
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=max_size_mb * 1024 * 1024,
            backupCount=backup_count
        )
        
        # Use a simpler formatter for file logs
        file_formatter = logging.Formatter(
            '%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(file_formatter)
        
        # Add the file handler to the root logger
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
add_file_logging(log, "h2tester.log")

class HTTP2Connection:
    def __init__(self, uri, method="GET", body=None, headers=None, ping_interval=1, request_interval=7):
        # Parse the URI
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
        
        # If there's a query string, append it to the path
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
                log.info(f"Connectingto {self.host}:{self.port} via HTTP/2 (gen {self.gen})")
                self.reader, self.writer = await asyncio.open_connection(self.host, self.port, ssl=ssl_context)
                # log.info(f"Connected to {self.host}:{self.port} via HTTP/2 (gen {self.gen})")
                log.info(f"Connected to {self.host}:{self.port} in {time.time() - start_time:.3f}s")

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
                            elif isinstance(event, h2.events.ConnectionTerminated):
                                # print(f"[!] Connection terminated by server: {event}")
                                raise Exception(f"Connection terminated by server: {event.error_code}")
                            else:
                                log.warning(f"Unhandled event: {event}")
                        
                        data = self.conn.data_to_send()
                        if data:
                            # print(f"Draining data {data}")
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
                # log.error(f"[!] {e} at {e.__traceback__.tb_frame.f_code.co_filename} line {e.__traceback__.tb_lineno}")
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
                # print(f"[?] Sent PING {ping_data}")
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
                    # ("user-agent", "async-http2-tester"),
                ]
                
                for name, value in self.req['headers'].items():
                    headers.append((name.lower(), value))
                end_stream = self.req['body'] is None
                self.conn.send_headers(stream_id, headers, end_stream=end_stream)
                
                if not end_stream:
                    self.conn.send_data(stream_id, self.req['body'], end_stream=True)
                
                self.request_active = True
                self.writer.write(self.conn.data_to_send())
                log.info(f"Sent request")
                await self.writer.drain()
                
                headers = {}
                data = b''

                while True:
                    event = await self.stream[stream_id].get()
                    # print(f"[>] {event} from {event.stream_id}")
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
        """Generate random 8-byte ping payload."""
        return bytes(random.choices(string.ascii_letters.encode(), k=8))

    async def close(self):
        """Close the connection gracefully."""
        if self.writer:
            self.conn.close_connection()
            self.writer.write(self.conn.data_to_send())
            await self.writer.drain()
            self.writer.close()
            await self.writer.wait_closed()
            print("[+] Connection closed.")

async def main():
    # Examples of how to use the new URI-based constructor
    # host = "example.com"
    # host = "127.0.0.1"
    # host = "nghttp2.org"
    
    # Simple GET request
    # https://ntapi-ha-gateway.test.env/ping
    # tester = HTTP2Connection("https://nghttp2.org/httpbin/get")
    # tester = HTTP2Connection("https://gitlab.test.env")
    # tester = HTTP2Connection("https://git-02.test.env")
    
    # tester = HTTP2Connection("https://gitlab-02.test.env")
    tester = HTTP2Connection("https://gitlab-dr.test.env")
    
    # tester = HTTP2Connection("https://ld-git-db.test.env")
    
    # POST request with body and headers
    # tester = HTTP2Connection(
    #     "https://example.com/api/data",
    #     method="POST",
    #     body=b'{"key": "value"}',
    #     headers={"Content-Type": "application/json"}
    # )
    log.info("Starting for %s", tester.req['uri'])
    await tester.connect()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Keyboard interrupt")

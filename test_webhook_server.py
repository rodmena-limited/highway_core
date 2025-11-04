#!/usr/bin/env python3
"""
Simple webhook test server for testing Highway Core webhook functionality.
"""

from http.server import HTTPServer, BaseHTTPRequestHandler
import json
import threading
import time
from urllib.parse import urlparse


class WebhookHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        # Parse the request body
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length).decode('utf-8')
        
        print(f"Received webhook request to {self.path}")
        print(f"Headers: {dict(self.headers)}")
        print(f"Body: {post_data}")
        print("-" * 50)
        
        # Respond with 200 OK
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        
        response = {"status": "received", "message": "Webhook received successfully"}
        self.wfile.write(json.dumps(response).encode('utf-8'))
    
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        
        response = {"message": "Webhook test server is running"}
        self.wfile.write(json.dumps(response).encode('utf-8'))


def run_webhook_server(port=7666):
    server = HTTPServer(('127.0.0.1', port), WebhookHandler)
    print(f"Starting webhook server on 127.0.0.1:{port}")
    server.serve_forever()


if __name__ == "__main__":
    print("Starting webhook test server...")
    print("This server will listen for webhooks on http://127.0.0.1:7666/index.json")
    print("To test, run the webhook workflow and then start the webhook runner with:")
    print("  python cli.py webhooks run")
    print()
    
    # Start server in a separate thread so we can handle Ctrl+C properly
    server_thread = threading.Thread(target=run_webhook_server, daemon=True)
    server_thread.start()
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nShutting down webhook server...")
        server_thread.join(timeout=1)
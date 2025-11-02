#!/bin/bash
# Debug script to test the TODO_ID extraction

# Simulate the todo_data result
TODO_DATA='{"status": 200, "data": {"userId": 10, "id": 199, "title": "numquam repellendus a magnam", "completed": true}, "headers": {"Date": "Sun, 02 Nov 2025 18:44:35 GMT", "Content-Type": "application/json; charset=utf-8", "Transfer-Encoding": "chunked", "Connection": "keep-alive", "access-control-allow-credentials": "true", "Cache-Control": "max-age=43200", "etag": "W/\"5f-NECuhAPBR69wysiYRXeF2dezUZg\"", "expires": "-1", "nel": "{\"report_to\":\"heroku-nel\",\"response_headers\":[\"Via\"],\"max_age\":3600,\"success_fraction\":0.01,\"failure_fraction\":0.1}", "pragma": "no-cache", "report-to": "{\"group\":\"heroku-nel\",\"endpoints\":[{\"url\":\"https://nel.heroku.com/reports?s=kC7BOvbzT7mTVCv%2B0cYoMH%2B1KSl7bydVnzwETxV7Jgk%3D\\u0026sid=e11707d5-02a7-43ef-b45e-2cf4d2036f7d\\u0026ts=1762103575\"}],\"max_age\":3600}", "reporting-endpoints": "heroku-nel=\"https://nel.heroku.com/reports?s=kC7BOvbzT7mTVCv%2B0cYoMH%2B1KSl7bydVnzwETxV7Jgk%3D&sid=e11707d5-02a7-43ef-b45e-2cf4d2036f7d&ts=1762103575\"", "Server": "cloudflare", "vary": "Origin, Accept-Encoding", "via": "2.0 heroku-router", "x-content-type-options": "nosniff", "x-powered-by": "Express", "x-ratelimit-limit": "1000", "x-ratelimit-remaining": "999", "x-ratelimit-reset": "1762103607", "Age": "5499", "cf-cache-status": "HIT", "Content-Encoding": "gzip", "CF-RAY": "9985c8b769cc949d-LHR", "alt-svc": "h3=\":443\"; ma=86400"}}'

echo "Original todo_data:"
echo "$TODO_DATA"
echo ""

# Test the extraction
echo "Testing extraction command:"
TODO_ID=$(echo "$TODO_DATA" | grep -o '"id":[[:space:]]*[0-9]*' | cut -d ':' -f 2 | tr -d ' ')
echo "Extracted TODO_ID: '$TODO_ID'"

if [ -z "$TODO_ID" ]; then
  echo "ERROR: Could not extract TODO_ID"
  exit 1
fi

echo "Using extracted ID: $TODO_ID"
echo "Simulated redis-cli command: redis-cli -h highway_redis GET todo:${TODO_ID}:title"
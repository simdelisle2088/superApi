@echo off

locust -f get-driver-order.py --host=http://127.0.0.1:8000 --users 384 --spawn-rate 32
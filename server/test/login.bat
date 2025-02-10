@echo off

locust -f login.py --host=http://127.0.0.1:8000 --users 128 --spawn-rate 16
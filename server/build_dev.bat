@echo off
REM Build Docker image for development
docker build -t deliver-dev:latest --build-arg PORT=8101 .
pause

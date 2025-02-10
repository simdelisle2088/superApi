@echo off
REM Build Docker image for production
docker build -t deliver-prod:latest --build-arg PORT=8011 .
pause

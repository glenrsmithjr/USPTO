# Pulls from base image with Python 3 already installed
FROM python:3

# Copy all files to app folder
COPY download_build/ /usr/src/app

# Switch working directory to app folder
WORKDIR /usr/src/app

# Download required packages (not utilizing any cache, if one exists)
RUN pip install --no-cache-dir -r requirements.txt

# Set command to run file
CMD ["python", "./main.py"]
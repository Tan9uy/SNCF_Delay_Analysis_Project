FROM python:3.8-slim-buster

WORKDIR /python-docker

# Install app dependencies
COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

# Copy source
COPY flask_api.py .

# Start
CMD [ "python3", "flask_api.py"]
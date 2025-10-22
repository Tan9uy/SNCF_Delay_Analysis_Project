FROM python:3.8-slim-buster

WORKDIR /python-docker

# Install app dependencies
COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

# Copy source
COPY app.py .

# Start
CMD [ "python3", "app.py"]
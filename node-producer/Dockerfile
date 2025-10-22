FROM node:16

# Create app directory
WORKDIR /usr/src/app

# Install app dependencies
COPY package*.json ./

RUN npm i --only=production

# Copy sources
COPY . .

# Start
CMD [ "node", "index.js" ]
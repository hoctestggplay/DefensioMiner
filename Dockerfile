FROM node:18-bullseye

# Cài Rust (cần để build solver)
RUN apt-get update && apt-get install -y curl build-essential pkg-config libssl-dev
RUN curl https://sh.rustup.rs -sSf | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

WORKDIR /app

COPY package*.json ./
RUN npm install

COPY . .

# Build solver Rust
RUN npm run build:solver

# Expose port nếu cần API (miner thì thường không cần)
EXPOSE 3000

CMD ["npm", "start"]

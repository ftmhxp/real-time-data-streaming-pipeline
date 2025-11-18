# Base image: Python 3.11 on Debian Bookworm
FROM mcr.microsoft.com/devcontainers/python:3.11-bookworm

# Install system dependencies
RUN apt-get update && apt-get install -y \
    git \
    curl \
    wget \
    unzip \
    openjdk-17-jdk \
    && apt-get clean

# Upgrade pip
RUN pip install --upgrade pip

# Install Docker CLI
RUN apt-get update && apt-get install -y \
    docker.io \
    && apt-get clean
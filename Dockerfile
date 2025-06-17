FROM ubuntu:20.04

# Set non-interactive mode for apt
ENV DEBIAN_FRONTEND=noninteractive

# Install dependencies
RUN apt-get update && \
    apt-get install -y \
        gcc \
        make \
        flex \
        bison \
        byacc \
        git \
        && rm -rf /var/lib/apt/lists/*

# Clone TPC-DS kit and build it
RUN git clone https://github.com/gregrahn/tpcds-kit.git /tpcds-kit && \
    cd /tpcds-kit/tools && \
    make OS=LINUX

COPY build.sh /tpcds-kit/tools/

# Set working directory
WORKDIR /tpcds-kit/tools


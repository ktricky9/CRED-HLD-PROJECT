FROM bitnami/spark:3.4.1

USER root

# Install Python and dependencies with debugging tools
RUN apt-get update && apt-get install -y python3 python3-pip curl net-tools iproute2 netcat procps && rm -rf /var/lib/apt/lists/*

RUN pip3 install pyspark==3.4.1 pandas minio kafka-python

# Set up app directory
WORKDIR /app
COPY app/ /app/

ENTRYPOINT ["python3", "/app/main.py"]

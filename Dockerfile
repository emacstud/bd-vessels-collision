FROM python:3.11-slim-bookworm

RUN apt-get update && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        curl \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN JAVA_BIN="$(readlink -f "$(which java)")" \
    && JAVA_HOME_PATH="$(dirname "$(dirname "$JAVA_BIN")")" \
    && ln -s "$JAVA_HOME_PATH" /usr/lib/jvm/default-java
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="$JAVA_HOME/bin:$PATH"

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/

VOLUME ["/app/data", "/app/output"]

CMD ["python", "-m", "src.main"]

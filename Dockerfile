FROM eclipse-temurin:17-jre AS java-base

FROM python:3.12-slim

COPY --from=java-base /opt/java/openjdk /opt/java/openjdk
ENV JAVA_HOME=/opt/java/openjdk
ENV PATH="${JAVA_HOME}/bin:${PATH}"

RUN apt-get update && \
    apt-get install -y --no-install-recommends procps && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN pip install --no-cache-dir poetry && \
    poetry config virtualenvs.create false

COPY pyproject.toml poetry.lock ./
RUN poetry install --no-interaction --no-ansi --no-root

COPY . .
RUN poetry install --no-interaction --no-ansi

CMD ["python", "-m", "billups_challenge.main"]

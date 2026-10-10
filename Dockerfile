ARG PYTHON_IMAGE
FROM ${PYTHON_IMAGE}
WORKDIR /app
COPY requirements.lock ./
RUN pip install --no-cache-dir -r requirements.lock
COPY pyproject.toml ./
COPY src ./src
RUN pip install --no-cache-dir --no-deps . && useradd --uid 10001 --create-home app
USER 10001
CMD ["adf-orchestrator", "serve", "--host", "0.0.0.0", "--port", "8081"]

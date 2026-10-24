FROM python:3.13.2-slim@sha256:6b3223eb4d93718828223966ad316909c39813dee3ee9395204940500792b740
WORKDIR /app
COPY requirements.lock .
RUN pip install --no-cache-dir --require-hashes -r requirements.lock
COPY generator.py .
CMD ["python", "generator.py"]

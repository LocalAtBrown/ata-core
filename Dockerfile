FROM python:3.9.15-slim as req
COPY pyproject.toml .
COPY poetry.lock .
RUN pip install poetry && poetry export --without-hashes -o requirements.txt

FROM python:3.9.15-slim as builder
# install building necessities here with apt-get install, e.g. apt-get update && apt-get install gcc (if needed)
COPY --from=req requirements.txt .
RUN pip install --user -r requirements.txt

FROM python:3.9.15-slim as runner
# install running necessities here with apt-get install, e.g. apt-get update && apt-get install libpq-dev (if needed)
COPY --from=builder /root/.local /root/.local
COPY . /src/
ENV PATH=/root/.local/bin:$PATH

FROM public.ecr.aws/docker/library/python:3.9.15-slim as req
COPY pyproject.toml .
COPY poetry.lock .
RUN pip install poetry && poetry export --without-hashes -o requirements.txt

FROM public.ecr.aws/docker/library/python:3.9.15-slim as builder
# Install aws-lambda-cpp build dependencies
RUN apt-get update && \
  apt-get install -y \
  g++ \
  make \
  cmake \
  unzip \
  libcurl4-openssl-dev
COPY --from=req requirements.txt .
RUN pip install --user -r requirements.txt awslambdaric==2.0.4

FROM public.ecr.aws/docker/library/python:3.9.15-slim as runner
# install running necessities here with apt-get install, e.g. apt-get update && apt-get install libpq-dev (if needed)
COPY --from=builder /root/.local /root/.local
COPY . /ata-pipeline0/
ENV PATH=/root/.local/bin:$PATH
WORKDIR /ata-pipeline0/src
ENTRYPOINT [ "/usr/local/bin/python", "-m", "awslambdaric" ]
CMD [ "main.handler" ]

FROM public.ecr.aws/docker/library/python:3.9.15-slim as req
COPY pyproject.toml .
COPY poetry.lock .
RUN pip install poetry && poetry export --without-hashes -o requirements.txt

FROM public.ecr.aws/lambda/python:3.9 as runner
COPY --from=req requirements.txt  .
RUN  pip3 install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"
COPY /ata_pipeline0/ ${LAMBDA_TASK_ROOT}/src
# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "src/main.handler" ]

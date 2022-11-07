# ata-pipeline0
Snowplow to Redshift data processing.

## Run

Since this code is containerized to run on Lambda, the main entrypoint isn't quite designed to run locally.
However, we can build the Docker image and run it locally, then ping it to try it out locally. We can, of course,
still test all the functions used by the main handler and run those separately (e.g. in a scratch file) if so desired.

### Docker

The `Dockerfile` in the root of the project is the correct target for building.

To build locally: `docker build -t {desired-name-and-tag} .`. For example: `docker build -t lnl/ata-p0:latest .`
Don't forget the `.` at the end to reference the `Dockerfile` in the current directory (assuming you run the command
from the root of the project).

To run the Docker image locally (with the Lambda set up):
`docker run --rm -p {local_port}:{container_port} {name-and-tag} {command}`.
For example: `docker run --rm -p 9000:8080 lnl/ata-p0:latest`. This will serve it on `localhost:9000`. To ping
the handler (and confirm it works), you can then make an HTTP request to it. For example:
`curl -XPOST "http://localhost:9000/2015-03-31/functions/function/invocations" -d '{"field1":"b", "field2":"a"}'`

## Development Tools

We use [Poetry](https://python-poetry.org/) to manage dependencies. It also helps with pinning dependency and python
versions. We also use [pre-commit](https://pre-commit.com/) with hooks for [isort](https://pycqa.github.io/isort/),
[black](https://github.com/psf/black), and [flake8](https://flake8.pycqa.org/en/latest/) for consistent code style and
readability. Note that this means code that doesn't meet the rules will fail to commit until it is fixed.

We also use [mypy](https://mypy.readthedocs.io/en/stable/index.html) for static type checking. This can be run manually,
and the CI runs it on PRs.

### Setup

1. [Install Poetry](https://python-poetry.org/docs/#installation).
2. Run `poetry install --no-root`
3. Make sure the virtual environment is active, then
4. Run `pre-commit install`

You're all set up! Your local environment should include all dependencies, including dev dependencies like `black`.
This is done with Poetry via the `poetry.lock` file. As for the containerized code, that still pulls dependencies from
`requirements.txt`. Any containerized dependency requirements need to be updated in `pyproject.toml` then exported to
`requirements.txt`.

### Run Code Format and Linting

To manually run isort, black, and flake8 all in one go, simply run `pre-commit run --all-files`.

### Run Static Type Checking

To manually run mypy, simply run `mypy` from the root directory of the project. It will use the default configuration
specified in the mypy.ini file.

### Update Dependencies

To update dependencies in your local environment, make changes to the `pyproject.toml` file then run `poetry update`.

## Tests

We use [pytest](https://docs.pytest.org) to run our tests. Simply run `pytest tests` or a specific
directory/file/module to run tests.
We run continuous integration (CI) via GitHub actions. We have actions to run MyPy and run the tests.

## Deployment

Deployment for the Local News Lab (LNL) is handled via AWS CodePipeline, defined elsewhere in the ata-infrastructure repo.
We use Docker to containerize our code. The LNL deploys this project on AWS Lambda.

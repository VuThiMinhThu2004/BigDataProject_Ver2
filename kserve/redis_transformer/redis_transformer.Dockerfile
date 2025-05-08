FROM python:3.9-slim-bullseye

COPY . .
# Install dependencies
RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir kserve redis argparse typing

RUN useradd kserve -m -u 1000 -d /home/kserve
USER 1000
ENTRYPOINT ["python", "-m", "redis_transformer"]
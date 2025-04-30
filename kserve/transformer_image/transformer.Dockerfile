FROM python:3.9-slim

RUN apt-get update \
&& apt-get install -y --no-install-recommends git
COPY . .
# Install dependencies
RUN pip install kserve redis


# Define entrypoint
CMD ["python", "-m", "transformer"]
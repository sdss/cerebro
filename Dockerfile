FROM python:3.11-slim-bullseye

WORKDIR /root

COPY . cerebro

RUN cd cerebro && pip install .

# Entry point is just the naked command. It is expected that
# the configuration file and profiles are defined using environment
# variables.
CMD [ "cerebro", "start", "--debug" ]

FROM python:3.10-alpine

WORKDIR /root

COPY . cerebro

RUN cd cerebro && pip install .

# Entry point is just the naked command. It is expected that
# the configuration file and profiles are defined using environment
# variables.
CMD [ "cerebro", "start", "--debug" ]

FROM python:3.11-slim-bullseye

MAINTAINER Jose Sanchez-Gallego, gallegoj@uw.edu

WORKDIR /root

COPY . cerebro

# RUN apt-get update && apt-get install -y git

RUN cd cerebro && pip install .

# Entry point is just the naked command. It is expected that
# the configuration file and profiles are defined using environment
# variables.
CMD [ "cerebro", "start", "--debug" ]

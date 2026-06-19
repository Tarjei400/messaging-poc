# Run the .NET scenario suite — the mirror of the TypeScript `npm run scenarios:*`
# scripts. From this folder:
#
#   make                  # in-memory reference (no broker needed)
#   make artemis          # against Artemis      (docker compose up -d artemis)
#   make rabbitmq         # against RabbitMQ     (docker compose up -d rabbitmq)
#   make scenarios-all    # every reachable broker
#   make fault            # fault-tolerance timeline (BROKER=in-memory)
#   make fault BROKER=artemis
#   make build | make test | make clean
#
RUNNER := src/Messaging.Runner
SOLUTION := MessagingComparison.sln
CONFIG ?= Debug
BROKER ?= in-memory

# The projects target net8.0; this lets them run on a newer installed runtime
# (e.g. only .NET 10 present). Harmless when the 8.0 runtime is available, and
# overridable: `make scenarios DOTNET_ROLL_FORWARD=Disable`.
export DOTNET_ROLL_FORWARD ?= Major

RUN := dotnet run --project $(RUNNER) -c $(CONFIG) --

.DEFAULT_GOAL := scenarios

.PHONY: scenarios artemis rabbitmq scenarios-artemis scenarios-rabbitmq scenarios-all fault build test clean help

## Run the suite against the in-memory reference adapter (no broker required).
scenarios:
	$(RUN) in-memory

## Run the suite against Artemis.
artemis scenarios-artemis:
	$(RUN) artemis

## Run the suite against RabbitMQ.
rabbitmq scenarios-rabbitmq:
	$(RUN) rabbitmq

## Run the suite against every reachable broker.
scenarios-all:
	$(RUN) all

## Narrate the fault-tolerance timeline (override with BROKER=artemis|rabbitmq).
fault:
	$(RUN) fault $(BROKER)

## Build the whole solution.
build:
	dotnet build $(SOLUTION) -c $(CONFIG)

## Run the unit tests.
test:
	dotnet test $(SOLUTION) -c $(CONFIG)

## Remove build output.
clean:
	dotnet clean $(SOLUTION) -c $(CONFIG)

## List targets.
help:
	@grep -E '^[a-zA-Z_-]+( [a-zA-Z_-]+)*:' $(MAKEFILE_LIST) | sed 's/:.*//' | sort -u

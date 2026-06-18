# RabbitMQ with the community delayed-message-exchange plugin enabled.
# The plugin ships as a separate .ez archive that must be dropped into the
# plugins directory and activated; the stock image does not include it.
FROM rabbitmq:3.13-management

# Version of the plugin must match the RabbitMQ 3.13.x series.
ARG PLUGIN_VERSION=3.13.0
ARG PLUGIN_URL=https://github.com/rabbitmq/rabbitmq-delayed-message-exchange/releases/download/v${PLUGIN_VERSION}/rabbitmq_delayed_message_exchange-${PLUGIN_VERSION}.ez

ADD ${PLUGIN_URL} /opt/rabbitmq/plugins/rabbitmq_delayed_message_exchange-${PLUGIN_VERSION}.ez

# ADD downloads as root with 0600 perms, which the `rabbitmq` runtime user
# cannot read (boot fails with eacces and the exchange type stays unknown).
# Make the archive world-readable so the node can load it.
RUN chmod 0644 /opt/rabbitmq/plugins/rabbitmq_delayed_message_exchange-${PLUGIN_VERSION}.ez

RUN rabbitmq-plugins enable --offline rabbitmq_delayed_message_exchange

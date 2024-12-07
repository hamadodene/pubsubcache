#!/bin/sh

# Set default values for any environment variables that may not be passed
PULSAR_SERVICE_URL=${PULSAR_SERVICE_URL:-"pulsar://localhost:6650"}
PULSAR_TOPIC=${PULSAR_TOPIC:-"pubsubcache"}
NODE_ID=${NODE_ID:-$(uname -n)}
RECONNECT_ATTEMPTS=${RECONNECT_ATTEMPTS:-3}
RECONNECT_INTERVAL=${RECONNECT_INTERVAL:-5000}
MAX_RECONNECT_INTERVAL=${MAX_RECONNECT_INTERVAL:-60000}

# Run the Java application with the system properties
java -Dpulsar.serviceUrl="$PULSAR_SERVICE_URL" \
     -Dpulsar.topic="$PULSAR_TOPIC" \
     -Dnode.id="$NODE_ID" \
     -Dreconnect.attempts="$RECONNECT_ATTEMPTS" \
     -Dreconnect.interval="$RECONNECT_INTERVAL" \
     -Dmax.reconnect.interval="$MAX_RECONNECT_INTERVAL" \
     -jar /pubsubcache.jar

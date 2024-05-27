package org.pubsubcache.pulsar;

import org.apache.pulsar.client.api.*;

public class PulsarClientManager {
    private final PulsarClient client;
    private final Producer<byte[]> producer;
    private final Consumer<byte[]> consumer;

    public PulsarClientManager(String serviceUrl, String topic) throws PulsarClientException {
        client = PulsarClient.builder()
                .serviceUrl(serviceUrl)
                .build();

        producer = client.newProducer()
                .topic(topic)
                .create();

        consumer = client.newConsumer()
                .topic(topic)
                .subscriptionName("cache-subscription")
                .subscribe();
    }

    public void sendMessage(String message) throws PulsarClientException {
        producer.send(message.getBytes());
    }

    public Message<byte[]> receiveMessage() throws PulsarClientException {
        return consumer.receive();
    }

    public void close() throws PulsarClientException {
        producer.close();
        consumer.close();
        client.close();
    }
}

package org.pubsubcache.pulsar;

import org.apache.pulsar.client.api.*;

import java.util.Random;

public class PulsarClientManager {
    private final PulsarClient client;
    private final Producer<byte[]> producer;
    private final Consumer<byte[]> consumer;

    public PulsarClientManager(String serviceUrl, String topic, String nodeId) throws PulsarClientException {
        client = PulsarClient.builder()
                .serviceUrl(serviceUrl)
                .build();

        producer = client.newProducer()
                .topic(topic)
                .create();

        consumer = client.newConsumer()
                .topic(topic)
                .subscriptionName(nodeId + new Random().nextInt())
                .subscribe();
    }

    public void sendMessage(byte[] message) throws PulsarClientException {
        producer.send(message);
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

package org.pubsubcache;

import org.apache.pulsar.client.api.PulsarClientException;
import org.pubsubcache.cache.CacheService;

import java.util.logging.Level;
import java.util.logging.Logger;

public class Main {
    private static final java.util.logging.Logger logger = Logger.getLogger(Main.class.getName());

    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Please provide a node identifier as an argument");
            System.exit(1);
        }

        String nodeId = args[0];
        String pulsarServiceUrl = "pulsar://localhost:6650";
        String topic = "pubsubcache";

        try {
            CacheService cacheService = new CacheService(pulsarServiceUrl, topic, nodeId);

            // Start listener for incoming Pulsar messages
            cacheService.startListener();

            // Example usage
            if ("node1".equals(nodeId)) {
                logger.info("Node 1 putting key1 into cache");
                cacheService.put("key1", "value1");
                Thread.sleep(5000); // Wait to ensure messages are processed
            }

            logger.log(Level.INFO, "Node {0} cached value: {1}", new Object[]{nodeId, cacheService.get("key1")});

        } catch (PulsarClientException | InterruptedException e) {
            logger.log(Level.SEVERE, "Exception encountered", e);
        }
    }
}
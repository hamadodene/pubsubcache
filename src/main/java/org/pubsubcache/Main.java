package org.pubsubcache;

import org.apache.pulsar.client.api.PulsarClientException;
import org.pubsubcache.cache.CacheService;

public class Main {
    public static void main(String[] args) {
        try {
            CacheService cacheService = new CacheService("pulsar://localhost:6650", "my-topic");
            // Start listener for incoming Pulsar messages
            cacheService.startListener();

            // Example usage
            // cacheService.put("key1", "value1");
            // System.out.println("Cached value: " + cacheService.get("key1"));
            // cacheService.invalidate("key1");
            // System.out.println("After invalidation: " + cacheService.get("key1"));
        } catch (PulsarClientException e) {
            e.printStackTrace();
        }
    }
}
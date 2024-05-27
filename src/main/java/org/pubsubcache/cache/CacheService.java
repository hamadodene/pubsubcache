package org.pubsubcache.cache;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;
import org.pubsubcache.pulsar.PulsarClientManager;

public class CacheService {
    private final CacheManager cacheManager;
    private final PulsarClientManager pulsarClientManager;

    public CacheService(String pulsarServiceUrl, String topic) throws PulsarClientException {
        cacheManager = new CacheManager();
        pulsarClientManager = new PulsarClientManager(pulsarServiceUrl, topic);
    }

    public void put(String key, Object value) throws PulsarClientException {
        cacheManager.put(key, value);
        pulsarClientManager.sendMessage("PUT:" + key + ":" + value.toString());
    }

    public Object get(String key) {
        return cacheManager.get(key);
    }

    public void invalidate(String key) throws PulsarClientException {
        cacheManager.invalidate(key);
        pulsarClientManager.sendMessage("INVALIDATE:" + key);
    }

    public void load(String key, Object value) {
        cacheManager.put(key, value);
    }

    public void startListener() throws PulsarClientException {
        new Thread(() -> {
            try {
                while (true) {
                    Message<byte[]> message = pulsarClientManager.receiveMessage();
                    String msgContent = new String(message.getData());
                    processMessage(msgContent);
                }
            } catch (PulsarClientException e) {
                e.printStackTrace();
            }
        }).start();
    }

    private void processMessage(String message) {
        String[] parts = message.split(":");
        String action = parts[0];
        String key = parts[1];

        switch (action) {
            case "PUT":
                String value = parts[2];
                cacheManager.put(key, value);
                break;
            case "INVALIDATE":
                cacheManager.invalidate(key);
                break;
            default:
                break;
        }
    }
}

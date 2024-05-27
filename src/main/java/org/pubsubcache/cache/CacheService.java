package org.pubsubcache.cache;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;
import org.pubsubcache.pulsar.PulsarClientManager;

import java.util.logging.Level;
import java.util.logging.Logger;

public class CacheService {
    private static final Logger logger = Logger.getLogger(CacheService.class.getName());

    private final CacheManager cacheManager;
    private final PulsarClientManager pulsarClientManager;

    private String nodeId;

    public CacheService(String pulsarServiceUrl, String topic, String nodeId) throws PulsarClientException {
        cacheManager = new CacheManager();
        this.nodeId = nodeId;
        pulsarClientManager = new PulsarClientManager(pulsarServiceUrl, topic, nodeId);
    }

    public void put(String key, Object value) throws PulsarClientException {
        cacheManager.put(key, value);
        pulsarClientManager.sendMessage("PUT:" + nodeId + ":" + key + ":" + value.toString());
        logger.info("PUT key: " + key + ", value: " + value);
    }

    public Object get(String key) {
        return cacheManager.get(key);
    }

    public void invalidate(String key) throws PulsarClientException {
        cacheManager.invalidate(key);
        pulsarClientManager.sendMessage("INVALIDATE:" + nodeId + ":" + key);
        logger.info("INVALIDATE key: " + key);
    }

    public void load(String key, Object value) {
        cacheManager.put(key, value);
    }

    public void startListener() throws PulsarClientException {
        new Thread(() -> {
            logger.log(Level.INFO, "Start pulsar topic listener..");
            try {
                while (true) {
                    Message<byte[]> message = pulsarClientManager.receiveMessage();
                    String msgContent = new String(message.getData());
                    processMessage(msgContent);
                }
            } catch (PulsarClientException e) {
                e.printStackTrace();
                logger.log(Level.SEVERE, "Error receiving message", e);
            }
        }).start();
    }

    private void processMessage(String message) {
        String[] parts = message.split(":");
        String action = parts[0];
        String senderNodeId = parts[1];
        String key = parts[2];

        // Ignore messages from the same node
        if (senderNodeId.equals(nodeId)) {
            return;
        }

        switch (action) {
            case "PUT":
                String value = parts[3];
                cacheManager.put(key, value);
                logger.info("Received PUT key: " + key + ", value: " + value);
                break;
            case "INVALIDATE":
                cacheManager.invalidate(key);
                logger.info("Received INVALIDATE key: " + key);
                break;
            default:
                break;
        }
    }
}

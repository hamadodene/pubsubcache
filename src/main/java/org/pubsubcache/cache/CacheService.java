package org.pubsubcache.cache;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;
import org.pubsubcache.pulsar.PulsarClientManager;

import java.io.IOException;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class CacheService {
    private static final Logger logger = Logger.getLogger(CacheService.class.getName());

    private final CacheManager cacheManager;
    private PulsarClientManager pulsarClientManager;

    private String nodeId;
    private String topic;
    private String pulsarServiceUrl;

    private int maxReconnectAttempts = 3;
    private long reconnectInterval = 5000; // 5 seconds

    private final ConcurrentMap<String, CompletableFuture<Object>> pendingFetches = new ConcurrentHashMap<>();

    public CacheService() throws IOException {
        cacheManager = new CacheManager();
        this.pulsarServiceUrl = System.getProperty("pulsar.serviceUrl");
        this.topic = System.getProperty("pulsar.topic");
        this.nodeId = System.getProperty("node.id");
        this.maxReconnectAttempts = Integer.parseInt(System.getProperty("reconnect.attempts"));
        this.reconnectInterval = Long.parseLong(System.getProperty("reconnect.interval"));

        pulsarClientManager = new PulsarClientManager(pulsarServiceUrl, topic, nodeId);
    }

    public void put(String key, Object value) throws PulsarClientException {
        cacheManager.put(key, value);
        try {
            pulsarClientManager.sendMessage("PUT:" + nodeId + ":" + key + ":" + value.toString());
        } catch (PulsarClientException e) {
            handleException("Failed to send PUT message", e);
        }
        logger.info("PUT key: " + key + ", value: " + value);
    }

    //Local get
    public Object get(String key) {
        return cacheManager.get(key);
    }

    // Remote fetch
    public Object fetch(String key) throws PulsarClientException, InterruptedException, ExecutionException, TimeoutException {
        Object value = cacheManager.get(key);
        if (value != null) {
            return value;
        }

        CompletableFuture<Object> future = new CompletableFuture<>();
        pendingFetches.put(key, future);
        try {
            pulsarClientManager.sendMessage("FETCH_REQUEST:" + nodeId + ":" + key);
        } catch (PulsarClientException e) {
            handleException("Failed to send FETCH_REQUEST message", e);
        }

        try {
            return future.get(5, TimeUnit.SECONDS);
        } finally {
            pendingFetches.remove(key);
        }
    }

    public void invalidate(String key) throws PulsarClientException {
        cacheManager.invalidate(key);
        try {
            pulsarClientManager.sendMessage("INVALIDATE:" + nodeId + ":" + key);
        } catch (PulsarClientException e) {
            handleException("Failed to send INVALIDATE message", e);
        }

        logger.info("INVALIDATE key: " + key);
    }

    public void load(String key, Object value) {
        cacheManager.put(key, value);
    }

    public void startListener() throws PulsarClientException {
        new Thread(() -> {
            logger.log(Level.INFO, "Start pulsar topic listener..");
            while (true) {
                try {
                    Message<byte[]> message = pulsarClientManager.receiveMessage();
                    String msgContent = new String(message.getData());
                    processMessage(msgContent);
                } catch (PulsarClientException e) {
                    logger.log(Level.SEVERE, "Failed to receive message, attempting to reconnect", e);
                    if (!attemptReconnect()) {
                        logger.log(Level.SEVERE, "Reconnection attempts failed, clearing cache");
                        cacheManager.invalidateAll();
                        break;
                    }
                }
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
            case "FETCH_REQUEST":
                Object result = cacheManager.get(key);
                if (result != null) {
                    try {
                        pulsarClientManager.sendMessage("FETCH_RESPONSE:" + nodeId + ":" + key + ":" + result + ":" + System.currentTimeMillis());
                    } catch (PulsarClientException e) {
                        logger.log(Level.SEVERE, "Error sending FETCH_RESPONSE", e);
                    }
                }
                break;
            case "FETCH_RESPONSE":
                if (pendingFetches.containsKey(key)) {
                    String responseValue = parts[3];
                    CompletableFuture<Object> fetchFuture = pendingFetches.get(key);
                    if (fetchFuture != null) {
                        cacheManager.put(key, responseValue); // Save the value in cache
                        fetchFuture.complete(responseValue);
                        pendingFetches.remove(key);
                    }
                }
                break;
            default:
                break;
        }
    }


    private void handleException(String message, PulsarClientException e) throws CacheInvalidationException {
        logger.log(Level.SEVERE, message, e);
        if (!attemptReconnect()) {
            logger.log(Level.SEVERE, "Reconnection attempts failed, clearing cache");
            cacheManager.invalidateAll();
            throw new CacheInvalidationException(message, e);
        }
    }


    private boolean attemptReconnect() {
        for (int attempt = 1; attempt <= maxReconnectAttempts; attempt++) {
            try {
                logger.log(Level.INFO,"Attempting to reconnect to Pulsar (attempt {0} of {1})", new Object[]{attempt, maxReconnectAttempts});
                pulsarClientManager.close();
                pulsarClientManager = new PulsarClientManager(pulsarServiceUrl, topic, nodeId);
                return true; // Reconnection successful
            } catch (PulsarClientException e) {
                logger.log(Level.SEVERE, "Reconnection attempt " + attempt + " failed:  ", e);
                try {
                    Thread.sleep(reconnectInterval);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        return false; // All reconnection attempts failed
    }
}

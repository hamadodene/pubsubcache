package org.pubsubcache.cache;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;
import org.pubsubcache.pulsar.PulsarClientManager;

import java.io.*;
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
    private long maxReconnectInterval = 60000;
    private boolean degradedMode;

    private final ConcurrentMap<String, CompletableFuture<Object>> pendingFetches = new ConcurrentHashMap<>();

    public CacheService() throws IOException {
        cacheManager = new CacheManager();
        this.pulsarServiceUrl = System.getProperty("pulsar.serviceUrl");
        this.topic = System.getProperty("pulsar.topic");
        this.nodeId = System.getProperty("node.id");
        this.maxReconnectAttempts = Integer.parseInt(System.getProperty("reconnect.attempts", String.valueOf(maxReconnectAttempts)));
        this.reconnectInterval = Long.parseLong(System.getProperty("reconnect.interval", String.valueOf(reconnectInterval)));
        this.maxReconnectInterval = Long.parseLong(System.getProperty("max.reconnect.interval", String.valueOf(maxReconnectInterval))); // Max interval for backoff

        pulsarClientManager = new PulsarClientManager(pulsarServiceUrl, topic, nodeId);
        this.degradedMode = false;
    }

    public void put(String key, Object value) throws PulsarClientException {
        cacheManager.put(key, value);
        if (!degradedMode) {
            CacheMessage message = new CacheMessage("PUT", nodeId, key, value.toString(), System.currentTimeMillis());
            try {
                sendMessageWithRetry(message, maxReconnectAttempts, reconnectInterval);
            } catch (PulsarClientException e) {
                handleException("Failed to send PUT message after retries", e);
            }
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
        if (!degradedMode) {
            CacheMessage message = new CacheMessage("FETCH_REQUEST", nodeId, key, null, System.currentTimeMillis());
            try {
                sendMessageWithRetry(message, maxReconnectAttempts, reconnectInterval);
            } catch (PulsarClientException e) {
                handleException("Failed to send FETCH_REQUEST message after retries", e);
            }
        }

        try {
            return future.get(5, TimeUnit.SECONDS);
        } finally {
            pendingFetches.remove(key);
        }
    }

    public void invalidate(String key) throws PulsarClientException {
        cacheManager.invalidate(key);
        if (!degradedMode) {
            CacheMessage message = new CacheMessage("INVALIDATE", nodeId, key, null, System.currentTimeMillis());
            try {
                sendMessageWithRetry(message, maxReconnectAttempts, reconnectInterval);
            } catch (PulsarClientException e) {
                handleException("Failed to send INVALIDATE message after retries", e);
            }
        }
        logger.info("INVALIDATE key: " + key + " from node: " + nodeId);
    }

    public void load(String key, Object value) {
        cacheManager.put(key, value);
    }

    public void startListener() throws PulsarClientException {
        new Thread(() -> {
            logger.log(Level.INFO, "Start pulsar topic listener..");
            while (true) {
                try {
                    if (!degradedMode) {
                        Message<byte[]> message = pulsarClientManager.receiveMessage();
                        CacheMessage cacheMessage = deserialize(message.getData());
                        processMessage(cacheMessage);
                    }
                } catch (ClassNotFoundException | PulsarClientException e) {
                    logger.log(Level.SEVERE, "Failed to receive message, attempting to reconnect", e);
                    if (!attemptReconnect()) {
                        logger.log(Level.SEVERE, "Reconnection attempts failed, entering degraded mode");
                        degradedMode = true;
                        notifyAdmin();
                    }
                } catch (IOException e) {
                    logger.log(Level.SEVERE, "Cannot deserialize message, please check...");
                }
            }
        }).start();
    }

    private void processMessage(CacheMessage message) {
        String action = message.getAction();
        String senderNodeId = message.getNodeId();
        String key = message.getKey();

        // Ignore messages from the same node
        if (senderNodeId.equals(nodeId)) {
            return;
        }

        switch (action) {
            case "PUT":
                cacheManager.put(key, message.getValue());
                logger.info("Received PUT key: " + key + ", value: " + message.getValue() + " from node: " + senderNodeId);
                break;
            case "INVALIDATE":
                cacheManager.invalidate(key);
                logger.info("Received INVALIDATE key: " + key + " from node: " + senderNodeId);
                break;
            case "FETCH_REQUEST":
                Object value = cacheManager.get(key);
                if (value != null) {
                    CacheMessage responseMessage = new CacheMessage("FETCH_RESPONSE", nodeId, key, value.toString(), System.currentTimeMillis());
                    try {
                        sendMessage(responseMessage);
                    } catch (PulsarClientException e) {
                        logger.log(Level.SEVERE, "Error sending FETCH_RESPONSE", e);
                    }
                }
                break;
            case "FETCH_RESPONSE":
                if (pendingFetches.containsKey(key)) {
                    CompletableFuture<Object> fetchFuture = pendingFetches.get(key);
                    if (fetchFuture != null) {
                        String messageValue = message.getValue();
                        cacheManager.put(key, messageValue); // Save the value in cache
                        fetchFuture.complete(messageValue);
                        pendingFetches.remove(key);
                    }
                }
                break;
            default:
                break;
        }
    }

    private void sendMessage(CacheMessage message) throws PulsarClientException {
        byte[] serializedMessage = serialize(message);
        pulsarClientManager.sendMessage(serializedMessage);
    }

    private byte[] serialize(CacheMessage message) throws PulsarClientException {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutputStream out = new ObjectOutputStream(bos)) {
            out.writeObject(message);
            return bos.toByteArray();
        } catch (IOException e) {
            throw new PulsarClientException("Failed to serialize message", e);
        }
    }

    private CacheMessage deserialize(byte[] data) throws IOException, ClassNotFoundException {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(data);
             ObjectInputStream in = new ObjectInputStream(bis)) {
            return (CacheMessage) in.readObject();
        }
    }

    private void handleException(String message, PulsarClientException e) throws CacheInvalidationException {
        logger.log(Level.SEVERE, message, e);
        if (!attemptReconnect()) {
            logger.log(Level.SEVERE, "Reconnection attempts failed, entering degraded mode");
            degradedMode = true;
            notifyAdmin();
            throw new CacheInvalidationException(message, e);
        }
    }

    private boolean attemptReconnect() {
        for (int attempt = 1; attempt <= maxReconnectAttempts; attempt++) {
            try {
                logger.log(Level.INFO, "Attempting to reconnect to Pulsar (attempt {0} of {1})", new Object[]{attempt, maxReconnectAttempts});
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

    private void sendMessageWithRetry(CacheMessage message, int maxAttempts, long initialInterval) throws PulsarClientException {
        long interval = initialInterval;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                sendMessage(message);
                return; // Message sent successfully, exit the method
            } catch (PulsarClientException e) {
                logger.log(Level.SEVERE, "Attempt {0} to send message failed: {1}", new Object[]{attempt, e});
                if (attempt == maxAttempts) {
                    throw e; // Re-throw the exception if max attempts reached
                }
                try {
                    Thread.sleep(interval);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
                interval = Math.min(interval * 2, maxReconnectInterval); // Exponential backoff
            }
        }
    }

    private void notifyAdmin() {
        logger.log(Level.SEVERE, "System is in degraded mode. Please check the Pulsar service.");
    }
}

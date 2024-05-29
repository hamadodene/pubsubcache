package org.pubsubcache.cache;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;
import org.pubsubcache.pulsar.PulsarClientManager;

import java.io.*;
import java.util.Set;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class CacheService {
    private static final Logger logger = Logger.getLogger(CacheService.class.getName());

    private final CacheManager cacheManager;
    private PulsarClientManager pulsarClientManager;
    private PulsarClientManager monitorPulsarClientManager;
    private String nodeId;
    private String topic;
    private String pulsarServiceUrl;

    private int maxReconnectAttempts = 3;
    private long reconnectInterval = 5000; // 5 seconds
    private long maxReconnectInterval = 60000;
    private boolean degradedMode;

    private static final String NODE_REGISTRATION_TOPIC = "node-registration";
    private final ConcurrentMap<String, CompletableFuture<Object>> pendingFetches = new ConcurrentHashMap<>();
    private ConcurrentMap<String, Set<String>> pendingInvalidations = new ConcurrentHashMap<>();

    public ConcurrentMap<String, Long> getActiveNodes() {
        return activeNodes;
    }

    private ConcurrentMap<String, Long> activeNodes = new ConcurrentHashMap<>();
    private ScheduledExecutorService heartbeatScheduler;
    private ExecutorService listenerExecutor;
    private ExecutorService nodeMonitorExecutor;

    private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors());


    public CacheService() throws IOException {
        cacheManager = new CacheManager();
        this.pulsarServiceUrl = System.getProperty("pulsar.serviceUrl");
        this.topic = System.getProperty("pulsar.topic");
        this.nodeId = System.getProperty("node.id");
        this.maxReconnectAttempts = Integer.parseInt(System.getProperty("reconnect.attempts", String.valueOf(maxReconnectAttempts)));
        this.reconnectInterval = Long.parseLong(System.getProperty("reconnect.interval", String.valueOf(reconnectInterval)));
        this.maxReconnectInterval = Long.parseLong(System.getProperty("max.reconnect.interval", String.valueOf(maxReconnectInterval))); // Max interval for backoff

        this.pulsarClientManager = new PulsarClientManager(pulsarServiceUrl, topic, nodeId);
        this.degradedMode = false;
        this.monitorPulsarClientManager = new PulsarClientManager(pulsarServiceUrl, NODE_REGISTRATION_TOPIC, nodeId);
    }

    private void registerNode() throws PulsarClientException {
        CacheMessage registrationMessage = new CacheMessage("NODE_REGISTRATION", nodeId, null, null, System.currentTimeMillis());
        sendMonitorMessage(registrationMessage);
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

    //We handle Eventually consistency...That is mean for some period we can have no consistency key
    // This will be accepted by application that want to use this library
    public void invalidate(String key) throws PulsarClientException {
        cacheManager.invalidate(key);
        if (!degradedMode) {
            CacheMessage message = new CacheMessage("INVALIDATE", nodeId, key, null, System.currentTimeMillis());
            pendingInvalidations.putIfAbsent(key, ConcurrentHashMap.newKeySet());
            try {
                sendMessageWithRetry(message, maxReconnectAttempts, reconnectInterval);
            } catch (PulsarClientException e) {
                handleException("Failed to send INVALIDATE message after retries", e);
            }

            // Schedule a task to check for unacknowledged invalidations
            scheduler.schedule(() -> {
                Set<String> acks = pendingInvalidations.get(key);
                logger.log(Level.INFO, "Start scheduler for monitor invalidate of key {0}", key);
                if (acks != null && acks.size() < getNumberOfNodes()) {
                    logger.log(Level.WARNING, "Not all nodes acknowledged INVALIDATE for key: {0}", key);
                    // Retry invalidate
                    try {
                        logger.log(Level.WARNING, nodeId + ": Retry invalidate sending for key: {0}", key);
                        sendMessageWithRetry(message, maxReconnectAttempts, reconnectInterval);
                    } catch (PulsarClientException e) {
                        logger.log(Level.SEVERE, "Retry failed for INVALIDATE message for key: {0}", new Object[]{key, e.getMessage()});
                    }
                    // mark node as inactive
                    markUnresponsiveNodes(acks);
                } else {
                    // All acks received, remove from pendingInvalidations
                    pendingInvalidations.remove(key);
                }
            }, 5, TimeUnit.SECONDS); // Check after 5 seconds
        }
        logger.info(nodeId + ": INVALIDATE key: " + key + " from node: " + nodeId);
    }

    public void load(String key, Object value) {
        cacheManager.put(key, value);
    }

    public void startListener() throws PulsarClientException {
        registerNode();
        listenerExecutor = Executors.newSingleThreadExecutor();
        listenerExecutor.submit(() -> {
            logger.log(Level.INFO, nodeId + ": Start pulsar topic listener..");
            while (!degradedMode && !Thread.currentThread().isInterrupted()) {
                try {
                    Message<byte[]> message = pulsarClientManager.receiveMessage();
                    CacheMessage cacheMessage = deserialize(message.getData());
                    processMessage(cacheMessage);
                } catch (ClassNotFoundException | PulsarClientException e) {
                    logger.log(Level.SEVERE, "Failed to receive message, attempting to reconnect", e);
                    if (e instanceof InterruptedException) {
                        Thread.currentThread().interrupt(); // Preserve interrupt status
                        break;
                    }
                    if (!attemptReconnect()) {
                        logger.log(Level.SEVERE, "Reconnection attempts failed, entering degraded mode");
                        degradedMode = true;
                        notifyAdmin();
                    }
                } catch (IOException e) {
                    logger.log(Level.SEVERE, "Cannot deserialize message, please check...");
                }
            }
        });

        // Periodically send heartbeats to indicate this node is active
        heartbeatScheduler = Executors.newSingleThreadScheduledExecutor();
        heartbeatScheduler.scheduleAtFixedRate(() -> {
            sendHeartbeat();
        }, 0, 5, TimeUnit.SECONDS); // Send heartbeat every 10 seconds
    }

    public void stopListener() {
        this.degradedMode = true;
        if (listenerExecutor != null && !listenerExecutor.isShutdown()) {
            listenerExecutor.shutdownNow();
        }
        if (heartbeatScheduler != null && !heartbeatScheduler.isShutdown()) {
            heartbeatScheduler.shutdownNow();
        }

        if (nodeMonitorExecutor != null && !nodeMonitorExecutor.isShutdown()) {
            nodeMonitorExecutor.shutdownNow();
        }

        try {
            this.pulsarClientManager.close();
        } catch (PulsarClientException e) {
            logger.log(Level.SEVERE, "Failed to close Pulsar client manager", e);
        }
    }

    public void startNodeMonitor() {
        nodeMonitorExecutor = Executors.newSingleThreadExecutor();
        nodeMonitorExecutor.submit(() -> {
            logger.log(Level.INFO, nodeId + ": Start node monitor listener..");
            while (!degradedMode && !Thread.currentThread().isInterrupted()) {
                try {
                    Message<byte[]> message = monitorPulsarClientManager.receiveMessage();
                    CacheMessage cacheMessage = deserialize(message.getData());
                    processRegistrationMessage(cacheMessage);
                } catch (PulsarClientException | ClassNotFoundException e) {
                    logger.log(Level.SEVERE, "Failed to receive message in node monitor", e);
                    if (e instanceof InterruptedException) {
                        Thread.currentThread().interrupt(); // Preserve interrupt status
                        break;
                    }
                } catch (IOException e) {
                    logger.log(Level.SEVERE, "Cannot deserialize message, please check...", e.getMessage());
                }
            }
        });
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
                logger.info(nodeId + ": Received PUT key: " + key + ", value: " + message.getValue() + " from node: " + senderNodeId);
                break;
            case "INVALIDATE":
                cacheManager.invalidate(key);
                sendInvalidateAck(senderNodeId, key);
                logger.info(nodeId + ": Received INVALIDATE key: " + key + " from node: " + senderNodeId);
                break;
            case "INVALIDATE_ACK":
                Set<String> acks = pendingInvalidations.get(key);
                if (acks != null) {
                    acks.add(senderNodeId);
                    if (acks.size() == getNumberOfNodes()) {
                        pendingInvalidations.remove(key);
                        logger.info(nodeId + ": All nodes have acknowledged INVALIDATE for key: " + key);
                    }
                }
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

    private void processRegistrationMessage(CacheMessage message) {
        String action = message.getAction();
        String senderNodeId = message.getNodeId();
        String value = message.getValue();

        // Ignore messages from the same node
        if (senderNodeId.equals(nodeId)) {
            return;
        }

        switch (action) {
            case "NODE_REGISTRATION":
                logger.log(Level.INFO, nodeId + ": receive node {0} registration..", senderNodeId);
                activeNodes.put(senderNodeId, System.currentTimeMillis());
                // Send immediate heartbeat in response to registration
                sendHeartbeat();
                break;
            case "NODE_HEARTBEAT":
                activeNodes.put(senderNodeId, System.currentTimeMillis());
                break;
            case "NODE_FAILURE":
                logger.log(Level.INFO, nodeId + ": receive node failure from: {0}", senderNodeId);
                activeNodes.remove(value);
                break;
            default:
                break;
        }

        // Remove nodes that haven't sent a heartbeat in the last 30 seconds
        long now = System.currentTimeMillis();
        activeNodes.entrySet().removeIf(entry -> now - entry.getValue() > 30000);
    }

    private void sendMessage(CacheMessage message) throws PulsarClientException {
        byte[] serializedMessage = serialize(message);
        pulsarClientManager.sendMessage(serializedMessage);
    }

    private void sendHeartbeat() {
        try {
            CacheMessage heartbeatMessage = new CacheMessage("NODE_HEARTBEAT", nodeId, null, null, System.currentTimeMillis());
            sendMonitorMessage(heartbeatMessage);
        } catch (PulsarClientException e) {
            logger.log(Level.SEVERE, "Failed to send heartbeat message", e);
        }
    }

    private void sendNodeFailure(String nodeId) {
        try {
            CacheMessage node_failure = new CacheMessage("NODE_FAILURE", this.nodeId, null, nodeId, System.currentTimeMillis());
            sendMonitorMessage(node_failure);
        } catch (PulsarClientException e) {
            logger.log(Level.SEVERE, "Failed to send node failure message message", e);
        }
    }

    private void sendInvalidateAck(String receiverNodeId, String key) {
        CacheMessage ackMessage = new CacheMessage("INVALIDATE_ACK", nodeId, key, null, System.currentTimeMillis());
        try {
            sendMessageWithRetry(ackMessage, maxReconnectAttempts, reconnectInterval);
        } catch (PulsarClientException e) {
            logger.log(Level.SEVERE, nodeId + ": Failed to send INVALIDATE_ACK message", e);
        }
    }

    private void markUnresponsiveNodes(Set<String> acks) {
        long now = System.currentTimeMillis();
        for (String nodeId : activeNodes.keySet()) {
            if (!acks.contains(nodeId)
                //&& (now - activeNodes.get(nodeId) > 3000)
            ) {
                activeNodes.remove(nodeId);
                logger.log(Level.WARNING, this.nodeId + ": Marked node {0} as inactive due to lack of response", nodeId);
                // Inform all nodes about the unresponsive node
                sendNodeFailure(nodeId);
            }
        }
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
        long interval = reconnectInterval;
        for (int attempt = 1; attempt <= maxReconnectAttempts; attempt++) {
            try {
                logger.log(Level.SEVERE, "Attempting to reconnect to Pulsar (attempt {0} of {1})", new Object[]{attempt, maxReconnectAttempts});
                pulsarClientManager.close();
                pulsarClientManager = new PulsarClientManager(pulsarServiceUrl, topic, nodeId);
                degradedMode = false; // Reconnection successful
                return true;
            } catch (PulsarClientException e) {
                logger.log(Level.SEVERE, "Reconnection attempt {0} failed: {1}", new Object[]{attempt, e});
                try {
                    Thread.sleep(interval);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
                interval = Math.min(interval * 2, maxReconnectInterval); // Exponential backoff
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
        logger.log(Level.WARNING, "System is in degraded mode. Please check the Pulsar service.");
    }

    public int getNumberOfNodes() {
        return activeNodes.size();
    }

    private void sendMonitorMessage(CacheMessage message) throws PulsarClientException {
        byte[] serializedMessage = serialize(message);
        monitorPulsarClientManager.sendMessage(serializedMessage);
    }
}

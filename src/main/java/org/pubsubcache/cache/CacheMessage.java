package org.pubsubcache.cache;

import java.io.Serializable;

public class CacheMessage implements Serializable {
    private static final long serialVersionUID = 1L;

    private String action;
    private String nodeId;
    private String key;
    private String value;
    private long timestamp;

    public CacheMessage() {
        // Default constructor
    }

    public CacheMessage(String action, String nodeId, String key, String value, long timestamp) {
        this.action = action;
        this.nodeId = nodeId;
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
    }

    // Getters and Setters

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}

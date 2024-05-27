package org.pubsubcache.cache;

import org.apache.pulsar.client.api.PulsarClientException;

public class CacheInvalidationException extends PulsarClientException {
    public CacheInvalidationException(String message, Throwable cause) {
        super(message, cause);
    }
}
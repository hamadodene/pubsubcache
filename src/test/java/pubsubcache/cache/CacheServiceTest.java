package pubsubcache.cache;

import org.apache.pulsar.client.api.PulsarClientException;
import org.junit.jupiter.api.*;
import org.pubsubcache.cache.CacheService;
import org.testcontainers.containers.PulsarContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS) //Reuse same instance of class for all test
public class CacheServiceTest {
    private PulsarContainer pulsarContainer;
    private CacheService cacheService;

    @BeforeAll
    public void setup() throws Exception {
        pulsarContainer = new PulsarContainer(DockerImageName.parse("apachepulsar/pulsar:3.2.3"));
        pulsarContainer.start();

        // Set system properties for testing
        System.setProperty("pulsar.serviceUrl", pulsarContainer.getPulsarBrokerUrl());
        System.setProperty("pulsar.topic", "test-topic");
        System.setProperty("node.id", "test-node");
        System.setProperty("reconnect.attempts", "3");
        System.setProperty("reconnect.interval", "1000");

        cacheService = new CacheService();
        cacheService.startListener();
    }

    @AfterAll
    public void tearDown() {
        if (pulsarContainer != null) {
            pulsarContainer.stop();
        }
    }

    @Test
    public void testPutAndGet() throws PulsarClientException, InterruptedException {
        String key = "testKey";
        String value = "testValue";

        cacheService.put(key, value);
        Object cachedValue = cacheService.get(key);

        assertEquals(value, cachedValue, "Cached value should match the put value");
    }

    @Test
    public void testInvalidate() throws PulsarClientException {
        String key = "testKey";
        String value = "testValue";

        cacheService.put(key, value);
        cacheService.invalidate(key);
        Object cachedValue = cacheService.get(key);

        assertNull(cachedValue, "Cached value should be null after invalidation");
    }

    @Test
    public void testFetch() throws PulsarClientException, InterruptedException, ExecutionException, TimeoutException {
        String key = "testKey";
        String value = "testValue";

        cacheService.put(key, value);
        Object fetchedValue = cacheService.fetch(key);

        assertEquals(value, fetchedValue, "Fetched value should match the put value");
    }

    // To do
    // Multi node PUT + Fetch
}

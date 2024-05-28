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
public class CacheServiceTestMultiNode {

    private PulsarContainer pulsarContainer;
    private CacheService cacheServiceNode1;
    private CacheService cacheServiceNode2;

    @BeforeAll
    public void setup() throws Exception {
        pulsarContainer = new PulsarContainer(DockerImageName.parse("apachepulsar/pulsar:2.9.1"));
        pulsarContainer.start();

        // Set common properties for testing
        System.setProperty("pulsar.serviceUrl", pulsarContainer.getPulsarBrokerUrl());
        System.setProperty("pulsar.topic", "test-topic");
        System.setProperty("reconnect.attempts", "3");
        System.setProperty("reconnect.interval", "1000");

        // Initialize node 1
        System.setProperty("node.id", "node1");
        cacheServiceNode1 = new CacheService();
        cacheServiceNode1.startListener();

        // Initialize node 2
        System.setProperty("node.id", "node2");
        cacheServiceNode2 = new CacheService();
        cacheServiceNode2.startListener();
    }

    @AfterAll
    public void tearDown() {
        if (pulsarContainer != null) {
            pulsarContainer.stop();
        }
    }

    @Test
    public void testPutAndGetAcrossNodes() throws PulsarClientException, InterruptedException {
        String key = "testKey";
        String value = "testValue";

        // Put value from node1
        cacheServiceNode1.put(key, value);

        // Ensure the value is propagated to node2
        Thread.sleep(1000); // Wait a moment to ensure propagation

        Object cachedValueNode2 = cacheServiceNode2.get(key);
        assertEquals(value, cachedValueNode2, "Cached value in node2 should match the put value from node1");
    }

    @Test
    public void testInvalidateAcrossNodes() throws PulsarClientException, InterruptedException {
        String key = "testKey";
        String value = "testValue";

        // Put value from node1
        cacheServiceNode1.put(key, value);

        // Ensure the value is propagated to node2
        Thread.sleep(1000); // Wait a moment to ensure propagation

        // Invalidate key from node2
        cacheServiceNode2.invalidate(key);

        // Ensure the key is invalidated in node1
        Thread.sleep(1000); // Wait a moment to ensure propagation

        Object cachedValueNode1 = cacheServiceNode1.get(key);
        assertNull(cachedValueNode1, "Cached value in node1 should be null after invalidation from node2");
    }

    @Test
    public void testFetchAcrossNodes() throws PulsarClientException, InterruptedException, ExecutionException, TimeoutException {
        String key = "testKey";
        String value = "testValue";

        // Put value from node1
        cacheServiceNode1.put(key, value);

        // Ensure the value is propagated to node2
        Thread.sleep(1000); // Wait a moment to ensure propagation

        // Fetch key from node2
        Object fetchedValueNode2 = cacheServiceNode2.fetch(key);

        assertEquals(value, fetchedValueNode2, "Fetched value in node2 should match the put value from node1");
    }
}

package pubsubcache.cache;

import org.apache.pulsar.client.api.PulsarClientException;
import org.junit.jupiter.api.*;
import org.pubsubcache.cache.CacheService;
import org.testcontainers.containers.PulsarContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.doThrow;

@TestInstance(TestInstance.Lifecycle.PER_CLASS) //Reuse same instance of class for all test
public class CacheServiceTestMultiNode {

    private PulsarContainer pulsarContainer;
    private CacheService cacheServiceNode1;
    private CacheService cacheServiceNode2;

    private CacheService cacheServiceNode3;


    @BeforeAll
    public void setup() throws Exception {
        pulsarContainer = new PulsarContainer(DockerImageName.parse("apachepulsar/pulsar:3.2.3"));
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
        cacheServiceNode1.startNodeMonitor();

        // Initialize node 2
        System.setProperty("node.id", "node2");
        cacheServiceNode2 = new CacheService();
        cacheServiceNode2.startListener();
        cacheServiceNode2.startNodeMonitor();

        // Initialize node 3
        System.setProperty("node.id", "node3");
        cacheServiceNode3 = new CacheService();
        cacheServiceNode3.startListener();
        cacheServiceNode3.startNodeMonitor();
    }

    @AfterAll
    public void tearDown() {
        if (pulsarContainer != null) {
            pulsarContainer.stop();
        }
    }

    @Test
    public void testNodeRegistration() throws InterruptedException {
        Thread.sleep(2000); // Wait a moment to ensure registration
        assertEquals(2, cacheServiceNode1.getNumberOfNodes(), "All nodes should be registered");
        assertEquals(2, cacheServiceNode2.getNumberOfNodes(), "All nodes should be registered");
        assertEquals(2, cacheServiceNode3.getNumberOfNodes(), "All nodes should be registered");
    }

    @Test
    public void testInvalidateAck() throws PulsarClientException, InterruptedException {
        String key = "testKey";
        String value = "testValue";

        // Put value from node1
        cacheServiceNode1.put(key, value);

        // Ensure the value is propagated to node2 and node3
        Thread.sleep(1000); // Wait a moment to ensure propagation

        // Invalidate key from node2
        cacheServiceNode2.invalidate(key);

        // Wait for ACKs
        Thread.sleep(2000); // Wait a moment to ensure ACKs are received

        // Check that the key has been invalidated in all nodes
        assertNull(cacheServiceNode1.get(key), "Key should be invalidated in node1");
        assertNull(cacheServiceNode3.get(key), "Key should be invalidated in node3");
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

        Object cachedValueNode3 = cacheServiceNode3.get(key);
        assertEquals(value, cachedValueNode3, "Cached value in node3 should match the put value from node1");
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

    @Test
    public void testNodeFailureNotification() throws Exception {
        String key = "testKey2";
        String value = "testValue2";

        // Put value from node1
        cacheServiceNode1.put(key, value);

        // Ensure the value is propagated to node2
        Thread.sleep(1000); // Wait a moment to ensure propagation


        // Simulate that node2 does not respond by stopping its listener thread
        cacheServiceNode2.stopListener();

        // Invalidate key from node1
        cacheServiceNode1.invalidate(key);


        // Wait for the scheduler to run and process the lack of response
        Thread.sleep(6000);

        // Check that a NODE_FAILURE message was sent
        assertFalse(cacheServiceNode1.getActiveNodes().containsKey("node2"), "Node2 should be marked as inactive in node1");
        assertFalse(cacheServiceNode3.getActiveNodes().containsKey("node2"), "Node2 should be marked as inactive in node3");
    }


}

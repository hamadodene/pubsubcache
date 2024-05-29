# Pub-Sub Cache Using Caffeine Cache and Apache Pulsar

This library implements a simple pub-sub cache using Apache Pulsar and Caffeine Cache. It is designed to demonstrate the principles of a distributed cache with data propagation and failure handling.

**Note**: This is not intended for production usage.

## Architecture

### Node Registration
- **Registration Message**: When a node starts, it sends a registration message to the other nodes.
- **Acknowledgment**: When a node receives a registration message, it saves the sender's node information and sends back its own status to the sender.
- **Heartbeat Mechanism**: Nodes periodically send heartbeat messages to indicate they are still active. These heartbeats help in detecting node failures.

### Client-side Local Data Storage
- **Caffeine Cache**: Each client uses Caffeine Cache to store local data efficiently.

### Data Propagation on PUT
- **PUT Operation**: When a client performs a PUT operation, the data is propagated to other clients via a Pulsar topic.
- **Topic Subscription**: All clients subscribed to this topic will receive the data and update their local cache accordingly.

### Fetch Request Propagation
- **FETCH Operation**: When a client performs a fetch request, the request is propagated to all nodes.
- **Response Handling**: If a node receives a fetch request and has the key locally, it sends the response back with the data to the requesting node.

### Local Load Operations
- **LOAD Operation**: When a client performs a load operation, the PUT is executed only locally without propagating the data to other nodes.

### Local Get Operations
- **GET Operation**: When a client performs a get operation, the GET is executed only locally to fetch data from the local cache.

### Invalidate Operation
- **INVALIDATE Operation**: When a client performs a key invalidation, the invalidation request is sent to all active nodes.
- **ACK Handling**: After sending the invalidation, the node waits for INVALIDATE ACKs from the active nodes before removing the invalidation from the pending list.
- **Failure Handling**: If a node does not respond to the invalidation request, it will be marked as unreachable, and the invalidation will be retried.

### Pulsar Client Failure
- **Retry Mechanism**: If the Pulsar client fails, the node will retry a specified number of attempts to reconnect.
- **Node Downtime**: If the retry attempts are exhausted, the node will be considered down and enter a degraded mode where any operation is permitted.
  - In that case, probably nodes need to be restarted after analyze the issue.

## Future Improvements
- **Enhanced Failure Handling**: Improve the handling of node failures and reconnection strategies.
- **Scalability**: Optimize the system for better performance and scalability in a production environment.
- **Security**: Implement security features to protect data and communication between nodes.

## How to Use
1. **Start Pulsar**: Ensure Apache Pulsar is running and accessible.
2. **Configure Nodes**: Set up each node with the required configuration, including Pulsar service URL and topic.
3. **Run Nodes**: Start each node, which will automatically register with other nodes and begin participating in the cache operations.

## Example
```java
public static void main(String[] args) {
        System.setProperty("pulsar.serviceUrl", pulsar://localhost:6650);
        System.setProperty("pulsar.topic", "test-topic");
        System.setProperty("reconnect.attempts", "3");
        System.setProperty("reconnect.interval", "1000");

        // Initialize node 1
        System.setProperty("node.id", "node1");
    try {
        CacheService cacheService = new CacheService();

        // Start listener for incoming Pulsar messages
        cacheService.startListener();

        // Example usage
        cacheService.put("key1", "value1");
        System.out.println("Cached value: " + cacheService.get("key1"));
        cacheService.invalidate("key1");
        System.out.println("After invalidation: " + cacheService.get("key1"));
    } catch (PulsarClientException e) {
        e.printStackTrace();
    }
}


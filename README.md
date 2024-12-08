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
```

## Running the Project with Docker
You can easily test this project using Docker.
The repository contains a Dockerfile and a docker-compose.yml file to facilitate deployment.

#### Prerequisites
- Docker installed on your machine
- Docker Compose installed

#### Building and Running with Docker Compos
Clone the repository: 
  ```
  git clone https://your-repo-url.git
  cd your-project-directory
  ```
Build the project using maven
  ```
  mvn clean install -DSkipTests
  ```
Build the docker image
  ```
  docker build -t pubsubcache .
  ```
Start the services using Docker Compose
  ```
  docker-compose up
  ```
This will build and start the necessary containers, including:
 - The application (pubsubcache) running on dynamic ports. 
 - Apache Pulsar for messaging

View running containers
```
# docker ps
CONTAINER ID   IMAGE                       COMMAND                  CREATED        STATUS                    PORTS                                            NAMES
bf930284f3ea   pubsubcache:latest          "/start.sh"              10 hours ago   Up 12 minutes             0.0.0.0:55042->8080/tcp                          pubsubcache-pubsubcache-2
3ceb84c1ac67   pubsubcache:latest          "/start.sh"              10 hours ago   Up 12 minutes             0.0.0.0:55043->8080/tcp                          pubsubcache-pubsubcache-3
e19cee8958ae   pubsubcache:latest          "/start.sh"              10 hours ago   Up 12 minutes             0.0.0.0:55044->8080/tcp                          pubsubcache-pubsubcache-1
be1fa41c7d2f   apachepulsar/pulsar:3.2.3   "bin/pulsar standalo…"   10 hours ago   Up 12 minutes (healthy)   0.0.0.0:6650->6650/tcp, 0.0.0.0:8080->8080/tcp   pubsubcache-pulsar-1
```
## Exposed APIs
The application exposes several APIs you can test. After starting the services, you can access them as follows:
```
Base URL: http://localhost:<dynamic_port>
```
Note: You can retrieve the dynamic port by running the docker ps command and checking the port mapping under the "PORTS" column. The output will display the host port assigned to your container

### Endpoints 

#### GET /get/{key}

Retrieve a value from the cache by key.

- URL: /api/cache/get/{key}
- Method: GET
- Response:
  - 200 OK: Returns the cached value.
  - 404 Not Found: If the key does not exist.

Example curl command:
```
curl -X GET http://localhost:<dynamic_port>/api/cache/get/sampleKey
```

#### GET /fetch/{key}

Fetch a value from the cache
- URL: /api/cache/fetch/{key}
- Method: GET
- Response:
  - 200 OK: Returns the fetched value.
  - 500 Internal Server Error: If fetching fails.

Example curl command:
```
# Load a key-value pair into the cache on one of the nodes
# This stores "sampleKey" with the value "sampleValue" on node1
curl -X POST "http://localhost:<node1_dynamic_port>/api/cache/load?key=sampleKey&value=sampleValue"

# Attempt to fetch the value from a different node
# This fetch request will search for "sampleKey" by contacting other nodes in the cluster if necessary
curl -X GET "http://localhost:<node_2dynamic_port>/api/cache/fetch/sampleKey"

```
#### POST /load
Load a key-value pair into the cache.

- URL: /api/cache/load?key={key}&value={value}
- Method: POST
- Response:
  - 200 OK: Confirmation message.

Example curl command:
```
curl -X POST "http://localhost:<dynamic_port>/api/cache/load?key=sampleKey&value=sampleValue"
```

#### POST /put
Put a key-value pair into the cache and publish the change via Pulsar.

- URL: /api/cache/put?key={key}&value={value}
- Method: POST
- Response:
  - 200 OK: Confirmation message.

Example curl command:
```
curl -X POST "http://localhost:<dynamic_port>/api/cache/put?key=sampleKey&value=sampleValue"
```

#### DELETE /invalidate/{key}
Invalidate (delete) a key from the cache.

- URL: /api/cache/invalidate/{key}
- Method: DELETE
- Response:
  - 200 OK: Confirmation message.
  - 500 Internal Server Error: If invalidation fails.

Example curl command:
```
curl -X DELETE http://localhost:<dynamic_port>/api/cache/invalidate/sampleKey
```

## Example api test

### 1. Test LOAD Operation
- **Action**: Load a key-value pair into Node1 using the `LOAD` API.
- **Verification**: Retrieve the same key using the `GET` API on Node1 to ensure the value is stored correctly.

### 2. Test FETCH Operation
- **Action**: Load a key-value pair into Node1.
- **Verification**: Use the `FETCH` API on Node3 to verify that the key-value pair loaded in Node1 can be retrieved by querying another node in the cluster.

### 3. Test PUT Operation
- **Action**: Store a key-value pair in Node1 using the `PUT` API.
- **Verification**: Use the `GET` API on any node in the cluster to check that the key-value pair exists.

### 4. Test INVALIDATE Operation
- **Action**: Store a key-value pair in Node1 using the `PUT` API, then invalidate it.
- **Verification**: Use the `GET` API to confirm that the key no longer exists after invalidation.

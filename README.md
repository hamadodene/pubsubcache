# Pub-Sub Cache Using Caffeine Cache and Apache Pulsar

This is a simple library for a pub-sub cache using Apache Pulsar.

**Note**: This is not intended for production usage.

## Architecture

- **Client-side Local Data Storage**: Each client uses Caffeine Cache to store local data.
- **Data Propagation on PUT**:
  - When a client performs a PUT operation, the data is propagated to other clients via a Pulsar topic.
  - All clients subscribed to this topic will receive the data.
- **Fetch Request Propagation**:
  - When a client performs a fetch request, the request is propagated to all nodes.
  - If a node receives a fetch request and has the key locally, it sends the response back with the data.
- **Local Load Operations**:
  - When a client performs a load operation, the PUT is executed only locally.
- **Local Get Operations**:
  - When a client performs a get operation, the GET is executed only locally.

### Pulsar Client Failure

If the Pulsar client fails, the node will retry a maximum number of attempts. If the attempts are exhausted, the node will be considered down, and the local cache will be invalidated.

**Note**: This can be improved in the future.

## To Do
- Implement unit tests
- Write multi-node tests
- Publish to Maven repository

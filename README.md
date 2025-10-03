# Redis Data Structure Equivalents in Apache Ignite 3

## Overview

Apache Ignite 3 provides Redis-like data structure operations through the Table API using KeyValueView and RecordView, not a dedicated Redis-compatible interface. This project demonstrates Redis data structures and key-value patterns implemented using Ignite 3's APIs.

## Supported Scenarios

### Core Redis Data Structures
1. **Key-Value Store** - Redis GET/SET equivalents
2. **Hash** - Redis HSET/HGET equivalents
3. **List** - Redis LPUSH/RPOP equivalents
4. **Queue** - Redis FIFO queue operations
5. **Set** - Redis SADD/SISMEMBER equivalents
6. **Sorted Set** - Redis ZADD/ZSCORE equivalents

### Advanced Key-Value Patterns
7. **Primitive Types** - Integer, double, boolean key-value operations
8. **POJO Objects** - Custom object serialization and storage
9. **Data Colocation** - Composite keys for automatic data colocation
10. **Serialization** - JSON and Java serialization patterns
11. **Large Objects** - Document and file storage with compression
12. **Record View** - API design comparison with KeyValueView

## Key Differences from Redis

- No dedicated Redis API: Ignite 3 uses table-based approach with KeyValueView
- Structured data: Works with typed tuples/POJOs rather than byte arrays
- Schema required: Table creation with defined columns needed
- ACID transactions: All operations support transactional semantics
- SQL integration: Same data accessible via SQL queries

| Aspect             | Redis                          | Apache Ignite 3                   |
| ------------------ | ------------------------------ | --------------------------------- |
| Data Model     | Key-value (byte arrays) | Structured tuples/POJOs           |
| Schema         | Schema-less                    | Schema required (table creation)  |
| Transactions   | Limited transaction support    | Full ACID transactions            |
| Type Safety    | Binary data                    | Strong typing for keys and values |
| Query Language | Redis commands only            | SQL integration available         |
| Distribution   | Single-node or cluster         | Designed for distributed clusters |
| Consistency    | Eventual consistency options   | Strong consistency guarantees     |

## Client APIs

The key-value operations are available in multiple client implementations:

- Java Client: `/modules/client/` (primary implementation)
- .NET Client: `/modules/platforms/dotnet/`
- Python Client: `/modules/platforms/python/`
- C++ Client: `/modules/platforms/cpp/`

## Data Structures

Each Redis data structure maps to a specific Ignite table schema using composite keys:

### Key-Value Store
- Table: `(key VARCHAR PRIMARY KEY, val VARCHAR)`
- Pattern: Direct key lookup

### Hash
- Table: `(hash_name VARCHAR, field VARCHAR, val VARCHAR, PRIMARY KEY (hash_name, field))`
- Pattern: Composite key (hash_name, field) for Redis hash field operations

### List
- Table: `(list_name VARCHAR, list_index INTEGER, val VARCHAR, PRIMARY KEY (list_name, list_index))`
- Pattern: Composite key (list_name, index) with element shifting for Redis list semantics

### Queue
- Table: `(queue_name VARCHAR, seq BIGINT, val VARCHAR, PRIMARY KEY (queue_name, seq))`
- Metadata: `(queue_name VARCHAR PRIMARY KEY, head_seq BIGINT, tail_seq BIGINT)`
- Pattern: Composite key (queue_name, sequence) with head/tail pointers for efficient FIFO

### Set
- Table: `(set_name VARCHAR, member VARCHAR, dummy_val BOOLEAN DEFAULT true, PRIMARY KEY (set_name, member))`
- Pattern: Composite key (set_name, member) with unique member constraint for Redis set semantics

### Sorted Set
- Table: `(zset_name VARCHAR, member VARCHAR, score DOUBLE, PRIMARY KEY (zset_name, member))`
- Pattern: Composite key (zset_name, member) with score as value for Redis sorted set operations

## Redis Command Mapping

| Pattern | Redis Command | Ignite 3 Equivalent | Method |
|---------|---------------|---------------------|--------|
| Key-Value | `GET key` | `kvView.get(null, key)` | Retrieve value by key |
| | `SET key value` | `kvView.put(null, key, value)` | Store key-value pair |
| | `EXISTS key` | `kvView.contains(null, key)` | Check if key exists |
| | `DEL key` | `kvView.remove(null, key)` | Remove key |
| Hash | `HSET hash field value` | `kvView.put(null, compositeKey, value)` | Set hash field |
| | `HGET hash field` | `kvView.get(null, compositeKey)` | Get hash field |
| | `HEXISTS hash field` | `kvView.contains(null, compositeKey)` | Check hash field exists |
| | `HDEL hash field` | `kvView.remove(null, compositeKey)` | Delete hash field |
| List | `LPUSH list item` | `pushLeft(kvView, list, item)` | Add to list head |
| | `RPUSH list item` | `pushRight(kvView, list, item)` | Add to list tail |
| | `LINDEX list index` | `getByIndex(kvView, list, index)` | Get list element |
| | `LPOP list` | `popLeft(kvView, list)` | Remove from list head |
| Queue | `LPUSH queue item` | `enqueue(kvView, metaView, queue, item)` | Add to queue |
| | `RPOP queue` | `dequeue(kvView, metaView, queue)` | Remove from queue |
| Set | `SADD set member` | `sadd(kvView, set, member)` | Add unique member |
| | `SISMEMBER set member` | `sismember(kvView, set, member)` | Check member exists |
| | `SREM set member` | `srem(kvView, set, member)` | Remove member |
| | `SCARD set` | `scard(kvView, set)` | Get member count |
| | `SMEMBERS set` | `smembers(kvView, set)` | List all members |
| | `SINTER set1 set2` | `sinter(kvView, set1, set2)` | Find common members |
| Sorted Set | `ZADD zset score member` | `zadd(kvView, zset, member, score)` | Add scored member |
| | `ZSCORE zset member` | `zscore(kvView, zset, member)` | Get member score |
| | `ZINCRBY zset incr member` | `zincrby(kvView, zset, member, incr)` | Increment member score |
| | `ZREM zset member` | `zrem(kvView, zset, member)` | Remove member |
| Primitive Types | `SET counter 100` | `kvView.put(null, key, intValue)` | Store integer counter |
| | `INCR counter` | `incrementCounter(kvView, "counter", 1)` | Increment counter value |
| | `SET flag true` | `kvView.put(null, key, boolValue)` | Store boolean flag |
| POJO Objects | `SET user:123 {json}` | `storeUser(kvView, "user:123", userObj)` | Store custom object |
| | `GET user:123` | `getUser(kvView, "user:123")` | Retrieve custom object |
| Colocation | `SET user:123 data` | `kvView.put(null, affinityKey, value)` | Store with affinity key |
| Serialization | `SET msg {json}` | `storeJson(kvView, "msg", jsonData)` | Store JSON data |
| | `SET obj {binary}` | `storeBinary(kvView, "obj", serialized)` | Store binary data |
| Large Objects | `SET doc {content}` | `storeDocument(kvView, "doc", compressed)` | Store compressed document |
| | `SET file {binary}` | `storeFile(kvView, "file", fileData)` | Store large file |
| Record View | Multiple operations | `recordView.insert(null, record)` | Single record operation |

---

## 1. Key-Value Store (IgniteKeyValueExample.java)

Basic Redis GET/SET operations using direct keys.

### Key Operations

```java
// Redis SET equivalent
Tuple key = Tuple.create().set("key", "user:123");
Tuple value = Tuple.create().set("val", "John Doe");
kvView.put(null, key, value);

// Redis GET equivalent
Tuple retrievedValue = kvView.get(null, key);
String result = retrievedValue.stringValue("val");

// Redis EXISTS equivalent
boolean exists = kvView.contains(null, key);

// Redis DEL equivalent
boolean removed = kvView.remove(null, key);
```

### Running the Example
```bash
./gradlew runKeyValue
```

## 2. Hash (IgniteHashExample.java)

Redis hash operations using composite keys (hash_name, field).

### Hash Operations

```java
// Redis HSET equivalent: HSET user:123 name "John Doe"
Tuple nameKey = Tuple.create()
        .set("hash_name", "user:123")
        .set("field", "name");
Tuple nameValue = Tuple.create().set("value", "John Doe");
kvView.put(null, nameKey, nameValue);

// Redis HGET equivalent: HGET user:123 name
Tuple retrievedName = kvView.get(null, nameKey);
String name = retrievedName.stringValue("value");

// Redis HEXISTS equivalent: HEXISTS user:123 name
boolean nameExists = kvView.contains(null, nameKey);

// Redis HDEL equivalent: HDEL user:123 age
boolean ageRemoved = kvView.remove(null, ageKey);
```

### Running the Example
```bash
./gradlew runHash
```

## 3. List (IgniteListExample.java)

Redis list operations using composite keys (list_name, index) with element shifting.

### List Operations

```java
// Redis LPUSH equivalent: Add to head of list
pushLeft(kvView, "tasks", "task1");

// Redis RPUSH equivalent: Add to tail of list
pushRight(kvView, "tasks", "task3");

// Redis LINDEX equivalent: Get element by index
String firstElement = getByIndex(kvView, "tasks", 0);

// Redis LPOP equivalent: Remove from head
String popped = popLeft(kvView, "tasks");
```

### Running the Example
```bash
./gradlew runList
```

## 4. Queue (IgniteQueueExample.java)

Redis queue operations using composite keys (queue_name, sequence) with FIFO semantics.

### Queue Operations

```java
// Enqueue (Redis LPUSH equivalent)
enqueue(kvView, metaView, "jobs", "job1");

// Dequeue (Redis RPOP equivalent) - FIFO
String job = dequeue(kvView, metaView, "jobs");

// Peek at next item without removing
String nextJob = peek(kvView, metaView, "jobs");

// Get queue size
long size = getQueueSize(metaView, "jobs");
```

### Running the Example
```bash
./gradlew runQueue
```

## 5. Sorted Set (IgniteSortedSetExample.java)

Redis sorted set operations using composite keys (zset_name, member) with scores as values.

### Sorted Set Operations

```java
// Redis ZADD equivalent: ZADD leaderboard 100 "player1"
zadd(kvView, "leaderboard", "player1", 100.0);

// Redis ZSCORE equivalent: ZSCORE leaderboard "player1"
Double score = zscore(kvView, "leaderboard", "player1");

// Redis ZINCRBY equivalent: ZINCRBY leaderboard 50 "player1"
double newScore = zincrby(kvView, "leaderboard", "player1", 50.0);

// Redis ZREM equivalent: ZREM leaderboard "player3"
boolean removed = zrem(kvView, "leaderboard", "player3");

// Redis ZCARD equivalent: Get member count
long cardinality = zcard(kvView, "leaderboard");
```

### Running the Example
```bash
./gradlew runSortedSet
```

## 6. Set (IgniteSetExample.java)

Redis set operations using composite keys (set_name, member) with unique member constraints.

### Set Operations

```java
// Redis SADD equivalent: SADD tags "java"
boolean added = sadd(kvView, "tags", "java");

// Redis SISMEMBER equivalent: SISMEMBER tags "java"
boolean exists = sismember(kvView, "tags", "java");

// Redis SREM equivalent: SREM tags "python"
boolean removed = srem(kvView, "tags", "python");

// Redis SCARD equivalent: SCARD tags
long cardinality = scard(kvView, "tags");

// Redis SMEMBERS equivalent: SMEMBERS tags
smembers(kvView, "tags");

// Redis SINTER equivalent: Find common members
sinter(kvView, "set1", "set2");
```

### Running the Example
```bash
./gradlew runSet
```

## 7. Primitive Types (IgnitePrimitiveTypesExample.java)

Native type support for integers, doubles, and booleans without string conversion overhead.

### Primitive Operations

```java
// Integer keys with string values (session data)
Tuple sessionKey = Tuple.create().set("session_id", 12345);
Tuple sessionData = Tuple.create().set("user_data", "user:alice|role:admin");
sessionView.put(null, sessionKey, sessionData);

// String keys with integer values (counters)
Tuple counterKey = Tuple.create().set("counter_name", "page_views");
Tuple counterValue = Tuple.create().set("count_value", 1000);
counterView.put(null, counterKey, counterValue);

// String keys with boolean values (feature flags)
Tuple flagKey = Tuple.create().set("feature_name", "dark_mode");
Tuple flagValue = Tuple.create().set("is_enabled", true);
flagView.put(null, flagKey, flagValue);

// String keys with double values (metrics)
Tuple metricKey = Tuple.create().set("metric_name", "avg_response_time");
Tuple metricValue = Tuple.create().set("metric_value", 245.7);
metricsView.put(null, metricKey, metricValue);
```

### Running the Example
```bash
./gradlew runPrimitiveTypes
```

## 8. POJO Objects (IgnitePojoExample.java)

Custom object storage with automatic Java serialization for complex domain models.

### POJO Operations

```java
// Store User objects with string keys
User alice = new User("alice", "alice@example.com", "Alice Smith", 28);
storeUser(userView, "user:alice", alice);

// Store Product objects with custom ProductKey
ProductKey laptopKey = new ProductKey("electronics", "laptop", "SKU123");
Product laptop = new Product("Gaming Laptop", "Gaming laptop with advanced graphics", 1299.99, 50);
storeProduct(productView, laptopKey, laptop);

// Store SessionData with composite SessionKey
SessionKey sessionKey = new SessionKey("alice", "web", "192.168.1.100");
SessionData session = new SessionData("alice", "admin", System.currentTimeMillis(), 3600000);
storeSession(sessionView, sessionKey, session);
```

### Running the Example
```bash
./gradlew runPojo
```

## 9. Data Colocation (IgniteColocationExample.java)

Composite key usage for automatic data colocation to reduce network hops and improve performance.

### Colocation Operations

```java
// Colocate user profiles with their orders using user_id
client.sql().execute(null,
    "CREATE TABLE orders (" +
    "order_id VARCHAR, user_id VARCHAR, order_amount DOUBLE, " +
    "PRIMARY KEY (order_id, user_id))");

// All user data stored on same node for efficient queries
storeUser(userView, "user_001", "Alice Johnson", "alice@example.com", 0);
storeOrder(orderView, "order_101", "user_001", "Gaming Laptop", 1299.99);
storeOrder(orderView, "order_102", "user_001", "Wireless Mouse", 29.99);

// Efficient retrieval of colocated data
displayUserWithOrders(userView, orderView, "user_001");
```

### Running the Example
```bash
./gradlew runColocation
```

## 10. Serialization (IgniteSerializationExample.java)

JSON and Java serialization patterns for cross-language compatibility and Kafka integration.

### Serialization Operations

```java
// JSON serialization for API responses
UserProfile profile = new UserProfile("alice", "Alice Johnson", "alice@example.com", 28, "admin");
String profileJson = toJson(profile);
storeJson(jsonView, "user:alice", profileJson);

// Java serialization for complex objects
ShoppingCart cart = new ShoppingCart("user_123");
cart.addItem("laptop", 1299.99, 1);
cart.addItem("mouse", 29.99, 2);
storeBinary(binaryView, "cart:user_123", cart);

// Kafka message processing with pre-serialized data
processKafkaMessage(messageView, "user-events", "user_001",
                   createUserEvent("user_001", "LOGIN", System.currentTimeMillis()));
```

### Running the Example
```bash
./gradlew runSerialization
```

## 11. Large Objects (IgniteLargeObjectsExample.java)

Document and file storage with compression for large content management. Demo uses smaller sizes due to default VARBINARY 64KB limit.

### Large Object Operations

```java
// Store large documents with compression (demo sized for 64KB VARBINARY limit)
String largeArticle = generateLargeArticle(20000); // ~20KB article
storeDocument(docView, "article:tech_trends_2024",
             "Tech Trends 2024|author:John Doe", largeArticle);

// Store binary files with metadata
byte[] imageData = generateSyntheticImageData(30 * 1024); // 30KB image
storeFile(fileView, "img_001", "profile_photo.jpg", "image/jpeg", imageData);

// Handle pre-compressed data efficiently
String xmlReport = generateXmlReport(20000);
storeCompressedData(compressedView, "report:monthly_sales", "xml", xmlReport, "gzip");

// Stream processing for large datasets
processCompressedDataStream(xmlData, "Processing XML report");
```

### Running the Example
```bash
./gradlew runLargeObjects
```

## 12. Record View (IgniteRecordViewExample.java)

Comparison of KeyValueView and RecordView API designs using the same table schema.

Both APIs work with identical table structures. KeyValueView separates key and value in method calls, while RecordView treats the entire row as a single record. RecordView simplifies operations when the primary key is part of the domain object.

### Record View Operations

```java
// Same table schema for both approaches
client.sql().execute(null,
    "CREATE TABLE user_profiles (" +
    "user_id VARCHAR PRIMARY KEY," +
    "name VARCHAR," +
    "email VARCHAR," +
    "age INTEGER," +
    "department VARCHAR)");

// KeyValueView approach (separate key and value parameters)
KeyValueView<Tuple, Tuple> kvView = client.tables()
    .table("user_profiles")
    .keyValueView();

Tuple key = Tuple.create().set("user_id", "alice");
Tuple value = Tuple.create()
    .set("name", "Alice Johnson")
    .set("email", "alice@example.com")
    .set("age", 28)
    .set("department", "Engineering");
kvView.put(null, key, value);

// RecordView approach (single record parameter with primary key)
RecordView<Tuple> recordView = client.tables()
    .table("user_profiles")
    .recordView();

Tuple record = Tuple.create()
    .set("user_id", "alice")  // Primary key included in record
    .set("name", "Alice Johnson")
    .set("email", "alice@example.com")
    .set("age", 28)
    .set("department", "Engineering");
recordView.insert(null, record);

// Retrieval differences
Tuple kvResult = kvView.get(null, key);  // Returns only value fields
Tuple recordResult = recordView.get(null, key);  // Returns complete record including key
```

### Running the Example
```bash
./gradlew runRecordView
```

## Summary

Apache Ignite 3 does not provide a direct Redis-compatible API. The `KeyValueView` interface offers equivalent functionality with additional benefits like ACID transactions, SQL integration, and strong consistency guarantees. The main difference is working with structured tables rather than key-value pairs, which provides more flexibility and type safety.

## References

- [Apache Ignite 3 Documentation](https://ignite.apache.org/docs/ignite3/latest/)
- [Table API Reference](https://ignite.apache.org/docs/ignite3/latest//developers-guide/table-api)
- [Java Client Documentation](https://ignite.apache.org/docs/ignite3/latest/developers-guide/clients/java)

## Running the Project

### Build
```bash
./gradlew build
```

### Run Individual Examples

#### Core Redis Data Structures
```bash
# Key-Value Store
./gradlew runKeyValue

# Hash operations
./gradlew runHash

# List operations
./gradlew runList

# Queue operations
./gradlew runQueue

# Set operations
./gradlew runSet

# Sorted Set operations
./gradlew runSortedSet
```

#### Advanced Key-Value Patterns
```bash
# Primitive type operations
./gradlew runPrimitiveTypes

# POJO object operations
./gradlew runPojo

# Data colocation patterns
./gradlew runColocation

# Serialization examples
./gradlew runSerialization

# Large object storage
./gradlew runLargeObjects

# Record View comparison
./gradlew runRecordView
```

### Run All Examples (default)
```bash
./gradlew run  # Runs all 12 examples sequentially
```

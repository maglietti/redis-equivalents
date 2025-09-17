# Redis Data Structure Equivalents in Apache Ignite 3

## Overview

Apache Ignite 3 provides Redis-like data structure operations through the Table API using KeyValueView, not a dedicated Redis-compatible interface. This project demonstrates 6 key Redis data structures implemented using Ignite 3's KeyValueView API with composite keys.

## Supported Data Structures

1. Key-Value Store - Redis GET/SET equivalents
2. Hash - Redis HSET/HGET equivalents
3. List - Redis LPUSH/RPOP equivalents
4. Queue - Redis FIFO queue operations
5. Set - Redis SADD/SISMEMBER equivalents
6. Sorted Set - Redis ZADD/ZSCORE equivalents

## Key Differences

- No dedicated Redis API: Ignite 3 uses table-based approach with KeyValueView
- Structured data: Works with typed tuples/POJOs rather than byte arrays
- Schema required: Must create table with defined columns first
- ACID transactions: All operations support transactional semantics
- SQL integration: Same data accessible via SQL queries

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

## Key Differences from Redis

| Aspect             | Redis                          | Apache Ignite 3                   |
| ------------------ | ------------------------------ | --------------------------------- |
| Data Model     | Key-value (byte arrays) | Structured tuples/POJOs           |
| Schema         | Schema-less                    | Schema required (table creation)  |
| Transactions   | Limited transaction support    | Full ACID transactions            |
| Type Safety    | Binary data                    | Strong typing for keys and values |
| Query Language | Redis commands only            | SQL integration available         |
| Distribution   | Single-node or cluster         | Built for distributed clusters    |
| Consistency    | Eventual consistency options   | Strong consistency guarantees     |

## Client APIs

The key-value operations are available in multiple client implementations:

- Java Client: `/modules/client/` (primary implementation)
- .NET Client: `/modules/platforms/dotnet/`
- Python Client: `/modules/platforms/python/`
- C++ Client: `/modules/platforms/cpp/`

## Composite Key Patterns

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

| Data Structure | Redis Command | Ignite 3 Equivalent | Method |
|----------------|---------------|---------------------|---------|
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

## Summary

While Apache Ignite 3 doesn't provide a direct Redis-compatible API, the `KeyValueView` interface offers equivalent functionality with additional benefits like ACID transactions, SQL integration, and strong consistency guarantees. The main difference is that you work with structured tables rather than key-value pairs, which provides more flexibility and type safety.

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

### Run All Examples (default)
```bash
./gradlew run  # Runs all 6 examples sequentially
```

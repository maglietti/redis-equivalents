# Redis GET and SET Equivalents in Apache Ignite 3

## Overview

Apache Ignite 3 provides Redis-like key-value operations through the **Table API using KeyValueView**, not a dedicated Redis-compatible interface. This document shows how to perform GET and SET operations equivalent to Redis commands.

## Key Differences

- **No dedicated Redis API**: Ignite 3 uses table-based approach with KeyValueView
- **Structured data**: Works with typed tuples/POJOs rather than simple byte arrays
- **Schema required**: Must create table with defined columns first
- **ACID transactions**: All operations support transactional semantics
- **SQL integration**: Same data accessible via SQL queries

## GET Operations (Redis GET equivalent)

### Basic GET Operation

```java
// Get the KeyValueView for a table
KeyValueView<Tuple, Tuple> kvView = client.tables().table("cache_table").keyValueView();

// Create a key tuple
Tuple key = Tuple.create().set("key", "user:123");

// GET operation - retrieve value by key
Tuple value = kvView.get(null, key); // null = no explicit transaction
```

### Asynchronous GET

```java
CompletableFuture<Tuple> valueFuture = kvView.getAsync(null, key);
```

### GET with POJO (Plain Old Java Objects)

```java
KeyValueView<AccountKey, Account> kvView = client.tables()
    .table("accounts")
    .keyValueView(AccountKey.class, Account.class);

AccountKey key = new AccountKey(123456);
Account value = kvView.get(null, key);
```

## SET Operations (Redis SET equivalent)

### Basic PUT Operation

```java
// Create key and value tuples
Tuple key = Tuple.create().set("key", "user:123");
Tuple value = Tuple.create().set("val", "John Doe");

// SET operation - store key-value pair
kvView.put(null, key, value); // null = no explicit transaction
```

### Asynchronous PUT

```java
CompletableFuture<Void> putFuture = kvView.putAsync(null, key, value);
```

### PUT with POJO

```java
AccountKey key = new AccountKey(123456);
Account value = new Account("Val", "Kulichenko", 100.00d);
kvView.put(null, key, value);
```

## Additional Key-Value Operations

Apache Ignite 3's KeyValueView provides many more operations than Redis:

### CHECK if key exists (Redis EXISTS)

```java
boolean exists = kvView.contains(null, key);
```

### REMOVE key (Redis DEL)

```java
boolean removed = kvView.remove(null, key);
```

### GET and SET atomically (Redis GETSET)

```java
Tuple oldValue = kvView.getAndPut(null, key, newValue);
```

### PUT if absent (Redis SETNX)

```java
boolean success = kvView.putIfAbsent(null, key, value);
```

### Batch Operations

```java
// GET multiple keys
Map<Tuple, Tuple> values = kvView.getAll(null, Arrays.asList(key1, key2, key3));

// SET multiple key-value pairs
Map<Tuple, Tuple> pairs = Map.of(key1, value1, key2, value2);
kvView.putAll(null, pairs);
```

## Complete Working Example

```java
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Tuple;

public class IgniteKeyValueExample {
    public static void main(String[] args) throws Exception {
        // Connect to Ignite cluster
        try (IgniteClient client = IgniteClient.builder()
                .addresses("127.0.0.1:10800")
                .build()) {

            // First, create a table (equivalent to Redis database/namespace)
            client.sql().execute(null,
                "CREATE TABLE IF NOT EXISTS cache_table (" +
                "key VARCHAR PRIMARY KEY," +
                "val VARCHAR)"
            );

            // Get KeyValueView for the table
            KeyValueView<Tuple, Tuple> kvView = client.tables()
                .table("cache_table")
                .keyValueView();

            // Redis SET equivalent
            Tuple key = Tuple.create().set("key", "user:123");
            Tuple value = Tuple.create().set("val", "John Doe");
            kvView.put(null, key, value);

            // Redis GET equivalent
            Tuple retrievedValue = kvView.get(null, key);
            String result = retrievedValue.stringValue("val");
            System.out.println("Retrieved: " + result); // Output: John Doe

            // Redis EXISTS equivalent
            boolean exists = kvView.contains(null, key);
            System.out.println("Key exists: " + exists); // Output: true

            // Redis DEL equivalent
            boolean removed = kvView.remove(null, key);
            System.out.println("Key removed: " + removed); // Output: true
        }
    }
}
```

## Key Differences from Redis

| Aspect             | Redis                          | Apache Ignite 3                   |
| ------------------ | ------------------------------ | --------------------------------- |
| **Data Model**     | Simple key-value (byte arrays) | Structured tuples/POJOs           |
| **Schema**         | Schema-less                    | Schema required (table creation)  |
| **Transactions**   | Limited transaction support    | Full ACID transactions            |
| **Type Safety**    | Binary data                    | Strong typing for keys and values |
| **Query Language** | Redis commands only            | SQL integration available         |
| **Distribution**   | Single-node or cluster         | Built for distributed clusters    |
| **Consistency**    | Eventual consistency options   | Strong consistency guarantees     |

## Client APIs

The key-value operations are available in multiple client implementations:

- **Java Client**: `/modules/client/` (primary implementation)
- **.NET Client**: `/modules/platforms/dotnet/`
- **Python Client**: `/modules/platforms/python/`
- **C++ Client**: `/modules/platforms/cpp/`

## Redis Command Mapping

| Redis Command       | Ignite 3 Equivalent                    | Method                |
| ------------------- | -------------------------------------- | --------------------- |
| `GET key`           | `kvView.get(null, key)`                | Retrieve value by key |
| `SET key value`     | `kvView.put(null, key, value)`         | Store key-value pair  |
| `EXISTS key`        | `kvView.contains(null, key)`           | Check if key exists   |
| `DEL key`           | `kvView.remove(null, key)`             | Remove key            |
| `GETSET key value`  | `kvView.getAndPut(null, key, value)`   | Atomic get and set    |
| `SETNX key value`   | `kvView.putIfAbsent(null, key, value)` | Set if not exists     |
| `MGET key1 key2...` | `kvView.getAll(null, keys)`            | Get multiple keys     |
| `MSET key1 val1...` | `kvView.putAll(null, pairs)`           | Set multiple pairs    |

## Summary

While Apache Ignite 3 doesn't provide a direct Redis-compatible API, the `KeyValueView` interface offers equivalent functionality with additional benefits like ACID transactions, SQL integration, and strong consistency guarantees. The main difference is that you work with structured tables rather than simple key-value pairs, which provides more flexibility and type safety.

## References

- [Apache Ignite 3 Documentation](https://ignite.apache.org/docs/ignite3/latest/)
- [Table API Reference](https://ignite.apache.org/docs/ignite3/latest//developers-guide/table-api)
- [Java Client Documentation](https://ignite.apache.org/docs/ignite3/latest/developers-guide/clients/java)

## Running the Project

- Build: `./gradlew build`
- Run: `./gradlew run`

Output:

```shell
Retrieved: John Doe
Key exists: true
Key removed: true
```

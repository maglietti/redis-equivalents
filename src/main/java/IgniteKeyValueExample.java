import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Tuple;

/**
 * Demonstrates Redis key-value operations using Apache Ignite 3 KeyValueView API.
 *
 * Maps Redis key-value operations to Ignite direct key operations:
 * - Redis key = Ignite simple key
 * - Redis value = Ignite value with single column table
 *
 * Uses single column primary key for Redis key-value semantics
 * while leveraging Ignite's KeyValueView API pattern.
 */
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
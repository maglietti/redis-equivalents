import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Tuple;

/**
 * Demonstrates Redis hash operations using Apache Ignite 3 KeyValueView API.
 *
 * Maps Redis hash operations to Ignite composite key operations:
 * - Redis hash key = Ignite composite key (hash_name, field)
 * - Redis field-value pairs = Ignite key-value pairs with composite keys
 *
 * Uses composite primary key (hash_name, field) to maintain Redis hash semantics
 * while leveraging Ignite's KeyValueView API pattern.
 */
public class IgniteHashExample {
    public static void main(String[] args) throws Exception {
        try (IgniteClient client = IgniteClient.builder()
                .addresses("127.0.0.1:10800")
                .build()) {

            client.sql().execute(null,
                "CREATE TABLE IF NOT EXISTS hash_table (" +
                "hash_name VARCHAR," +
                "field VARCHAR," +
                "val VARCHAR," +
                "PRIMARY KEY (hash_name, field))"
            );

            KeyValueView<Tuple, Tuple> kvView = client.tables()
                    .table("hash_table")
                    .keyValueView();

            String hashName = "user:123";

            // Redis HSET equivalent: HSET user:123 name "John Doe"
            Tuple nameKey = Tuple.create()
                    .set("hash_name", hashName)
                    .set("field", "name");
            Tuple nameValue = Tuple.create().set("val", "John Doe");
            kvView.put(null, nameKey, nameValue);
            System.out.println("HSET " + hashName + " name 'John Doe'");

            // Redis HSET equivalent: HSET user:123 age 30
            Tuple ageKey = Tuple.create()
                    .set("hash_name", hashName)
                    .set("field", "age");
            Tuple ageValue = Tuple.create().set("val", "30");
            kvView.put(null, ageKey, ageValue);
            System.out.println("HSET " + hashName + " age '30'");

            // Redis HGET equivalent: HGET user:123 name
            Tuple retrievedName = kvView.get(null, nameKey);
            if (retrievedName != null) {
                String name = retrievedName.stringValue("val");
                System.out.println("HGET " + hashName + " name -> " + name);
            }

            // Redis HGET equivalent: HGET user:123 age
            Tuple retrievedAge = kvView.get(null, ageKey);
            if (retrievedAge != null) {
                String age = retrievedAge.stringValue("val");
                System.out.println("HGET " + hashName + " age -> " + age);
            }

            // Redis HEXISTS equivalent: HEXISTS user:123 name
            boolean nameExists = kvView.contains(null, nameKey);
            System.out.println("HEXISTS " + hashName + " name -> " + nameExists);

            // Redis HDEL equivalent: HDEL user:123 age
            boolean ageRemoved = kvView.remove(null, ageKey);
            System.out.println("HDEL " + hashName + " age -> " + ageRemoved);

            // Verify age field was deleted
            boolean ageStillExists = kvView.contains(null, ageKey);
            System.out.println("HEXISTS " + hashName + " age -> " + ageStillExists);
        }
    }
}
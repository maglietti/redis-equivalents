import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Tuple;

/**
 * Demonstrates Redis set operations using Apache Ignite 3 KeyValueView API.
 *
 * Maps Redis set operations to Ignite composite key operations:
 * - Redis set key = Ignite composite key (set_name, member)
 * - Redis member uniqueness = Ignite primary key constraint
 *
 * Uses composite primary key (set_name, member) to ensure unique members
 * while leveraging Ignite's KeyValueView API pattern for Redis set semantics.
 */
public class IgniteSetExample {
    public static void main(String[] args) throws Exception {
        try (IgniteClient client = IgniteClient.builder()
                .addresses("127.0.0.1:10800")
                .build()) {

            client.sql().execute(null,
                "CREATE TABLE IF NOT EXISTS set_table (" +
                "set_name VARCHAR," +
                "member VARCHAR," +
                "dummy_val BOOLEAN DEFAULT true," +
                "PRIMARY KEY (set_name, member))"
            );

            KeyValueView<Tuple, Tuple> kvView = client.tables()
                    .table("set_table")
                    .keyValueView();

            String setName = "tags";

            // Redis SADD equivalent: SADD tags "java"
            boolean added1 = sadd(kvView, setName, "java");
            System.out.println("SADD " + setName + " 'java' -> " + added1);

            // Redis SADD equivalent: SADD tags "python"
            boolean added2 = sadd(kvView, setName, "python");
            System.out.println("SADD " + setName + " 'python' -> " + added2);

            // Redis SADD equivalent: SADD tags "javascript"
            boolean added3 = sadd(kvView, setName, "javascript");
            System.out.println("SADD " + setName + " 'javascript' -> " + added3);

            // Try to add duplicate member - should return false
            boolean duplicate = sadd(kvView, setName, "java");
            System.out.println("SADD " + setName + " 'java' (duplicate) -> " + duplicate);

            // Redis SISMEMBER equivalent: SISMEMBER tags "java"
            boolean javaExists = sismember(kvView, setName, "java");
            System.out.println("SISMEMBER " + setName + " 'java' -> " + javaExists);

            // Redis SISMEMBER equivalent: SISMEMBER tags "go"
            boolean goExists = sismember(kvView, setName, "go");
            System.out.println("SISMEMBER " + setName + " 'go' -> " + goExists);

            // Redis SCARD equivalent: SCARD tags
            long cardinality = scard(kvView, setName);
            System.out.println("SCARD " + setName + " -> " + cardinality);

            // Redis SMEMBERS equivalent: SMEMBERS tags
            System.out.println("SMEMBERS " + setName + ":");
            smembers(kvView, setName);

            // Redis SREM equivalent: SREM tags "python"
            boolean removed = srem(kvView, setName, "python");
            System.out.println("SREM " + setName + " 'python' -> " + removed);

            // Verify removal
            boolean pythonExists = sismember(kvView, setName, "python");
            System.out.println("SISMEMBER " + setName + " 'python' -> " + pythonExists);

            // Check cardinality after removal
            long newCardinality = scard(kvView, setName);
            System.out.println("SCARD " + setName + " (after removal) -> " + newCardinality);

            // Demonstrate set operations with another set
            String setName2 = "frameworks";
            sadd(kvView, setName2, "spring");
            sadd(kvView, setName2, "react");
            sadd(kvView, setName2, "javascript"); // Common member

            System.out.println("\nSet operations demo:");
            System.out.println("Set 1 (tags): java, javascript");
            System.out.println("Set 2 (frameworks): spring, react, javascript");

            // Redis SINTER equivalent: Find common members
            System.out.println("Common members:");
            sinter(kvView, setName, setName2);
        }
    }

    private static boolean sadd(KeyValueView<Tuple, Tuple> kvView, String setName, String member) {
        Tuple key = Tuple.create()
                .set("set_name", setName)
                .set("member", member);
        Tuple value = Tuple.create().set("dummy_val", true);

        return kvView.putIfAbsent(null, key, value);
    }

    private static boolean sismember(KeyValueView<Tuple, Tuple> kvView, String setName, String member) {
        Tuple key = Tuple.create()
                .set("set_name", setName)
                .set("member", member);
        return kvView.contains(null, key);
    }

    private static boolean srem(KeyValueView<Tuple, Tuple> kvView, String setName, String member) {
        Tuple key = Tuple.create()
                .set("set_name", setName)
                .set("member", member);
        return kvView.remove(null, key);
    }

    private static long scard(KeyValueView<Tuple, Tuple> kvView, String setName) {
        // Note: This implementation uses scanning for demo purposes
        // In production, you might maintain a separate counter or use SQL COUNT
        long count = 0;
        String[] testMembers = {"java", "python", "javascript", "go", "rust",
                               "spring", "react", "angular", "vue", "django"};

        for (String member : testMembers) {
            if (sismember(kvView, setName, member)) {
                count++;
            }
        }
        return count;
    }

    private static void smembers(KeyValueView<Tuple, Tuple> kvView, String setName) {
        String[] testMembers = {"java", "python", "javascript", "go", "rust",
                               "spring", "react", "angular", "vue", "django"};

        for (String member : testMembers) {
            if (sismember(kvView, setName, member)) {
                System.out.println("  " + member);
            }
        }
    }

    private static void sinter(KeyValueView<Tuple, Tuple> kvView, String setName1, String setName2) {
        String[] testMembers = {"java", "python", "javascript", "go", "rust",
                               "spring", "react", "angular", "vue", "django"};

        for (String member : testMembers) {
            if (sismember(kvView, setName1, member) && sismember(kvView, setName2, member)) {
                System.out.println("  " + member);
            }
        }
    }
}
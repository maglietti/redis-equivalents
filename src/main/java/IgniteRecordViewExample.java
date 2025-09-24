import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Tuple;

/**
 * Demonstrates Record View API compared to Key-Value View API in Apache Ignite 3.
 *
 * Redis requires separate keys for related data causing duplication,
 * while Ignite's Record View eliminates this by storing complete records
 * with primary keys as part of the record structure.
 *
 * This pattern reduces memory usage and simplifies data access patterns
 * for applications with structured domain objects.
 */
public class IgniteRecordViewExample {
    public static void main(String[] args) throws Exception {
        try (IgniteClient client = IgniteClient.builder()
                .addresses("127.0.0.1:10800")
                .build()) {

            demonstrateUserProfileComparison(client);
            demonstrateProductCatalogComparison(client);
            demonstratePerformanceComparison(client);
        }
    }

    /**
     * Compares Key-Value vs Record View for user profile storage.
     * Shows data duplication elimination and simplified access patterns.
     */
    private static void demonstrateUserProfileComparison(IgniteClient client) throws Exception {
        System.out.println("=== User Profile: Key-Value vs Record View Comparison ===");

        // Create tables for both approaches
        createUserProfileTables(client);

        // Get views for both approaches
        KeyValueView<Tuple, Tuple> kvView = client.tables()
                .table("user_profiles_kv")
                .keyValueView();

        RecordView<Tuple> recordView = client.tables()
                .table("user_profiles_record")
                .recordView();

        // Store user data using Key-Value approach (with duplication)
        System.out.println("--- Key-Value Approach (with data duplication) ---");
        storeUserWithKeyValue(kvView, "alice", "Alice Johnson", "alice@example.com", 28, "Engineering");
        storeUserWithKeyValue(kvView, "bob", "Bob Smith", "bob@example.com", 34, "Marketing");

        // Store user data using Record View approach (no duplication)
        System.out.println("--- Record View Approach (no data duplication) ---");
        storeUserWithRecord(recordView, "alice", "Alice Johnson", "alice@example.com", 28, "Engineering");
        storeUserWithRecord(recordView, "bob", "Bob Smith", "bob@example.com", 34, "Marketing");

        // Retrieve and compare approaches
        System.out.println("--- Data Retrieval Comparison ---");
        Tuple kvUser = getUserWithKeyValue(kvView, "alice");
        Tuple recordUser = getUserWithRecord(recordView, "alice");

        System.out.println("Key-Value result: " + formatUserTuple(kvUser));
        System.out.println("Record result: " + formatUserTuple(recordUser));

        // Demonstrate update operations
        System.out.println("--- Update Operations Comparison ---");
        updateUserEmailKeyValue(kvView, "alice", "alice.johnson@company.com");
        updateUserEmailRecord(recordView, "alice", "alice.johnson@company.com");

        System.out.println();
    }

    /**
     * Compares Key-Value vs Record View for product catalog management.
     * Shows query pattern differences and data organization benefits.
     */
    private static void demonstrateProductCatalogComparison(IgniteClient client) throws Exception {
        System.out.println("=== Product Catalog: Key-Value vs Record View Comparison ===");

        // Create tables for both approaches
        createProductCatalogTables(client);

        KeyValueView<Tuple, Tuple> kvView = client.tables()
                .table("products_kv")
                .keyValueView();

        RecordView<Tuple> recordView = client.tables()
                .table("products_record")
                .recordView();

        // Store product data
        System.out.println("--- Storing Product Catalog Data ---");
        storeProductsKeyValue(kvView);
        storeProductsRecord(recordView);

        // Demonstrate query patterns
        System.out.println("--- Query Pattern Comparison ---");
        Tuple kvProduct = getProductWithKeyValue(kvView, "PROD001");
        Tuple recordProduct = getProductWithRecord(recordView, "PROD001");

        System.out.println("Key-Value product: " + formatProductTuple(kvProduct));
        System.out.println("Record product: " + formatProductTuple(recordProduct));

        // Demonstrate batch operations
        System.out.println("--- Batch Operations ---");
        updateProductPricesKeyValue(kvView);
        updateProductPricesRecord(recordView);

        System.out.println();
    }

    /**
     * Demonstrates performance and memory usage differences.
     * Shows how Record View reduces overhead and simplifies operations.
     */
    private static void demonstratePerformanceComparison(IgniteClient client) throws Exception {
        System.out.println("=== Performance and Memory Usage Comparison ===");

        // Create tables for performance testing
        createPerformanceTables(client);

        KeyValueView<Tuple, Tuple> kvView = client.tables()
                .table("perf_test_kv")
                .keyValueView();

        RecordView<Tuple> recordView = client.tables()
                .table("perf_test_record")
                .recordView();

        // Measure Key-Value operations
        System.out.println("--- Key-Value Performance Test ---");
        long kvStartTime = System.currentTimeMillis();
        performKeyValueOperations(kvView, 1000);
        long kvEndTime = System.currentTimeMillis();
        System.out.println("Key-Value operations completed in: " + (kvEndTime - kvStartTime) + "ms");

        // Measure Record View operations
        System.out.println("--- Record View Performance Test ---");
        long recordStartTime = System.currentTimeMillis();
        performRecordOperations(recordView, 1000);
        long recordEndTime = System.currentTimeMillis();
        System.out.println("Record operations completed in: " + (recordEndTime - recordStartTime) + "ms");

        // Memory usage analysis
        System.out.println("--- Memory Usage Analysis ---");
        analyzeMemoryUsage();

        System.out.println();
    }

    // Table creation methods
    private static void createUserProfileTables(IgniteClient client) throws Exception {
        // Key-Value table (user_id duplicated in key and value)
        client.sql().execute(null,
                "CREATE TABLE IF NOT EXISTS user_profiles_kv (" +
                        "user_id VARCHAR PRIMARY KEY," +
                        "user_id_dup VARCHAR," +  // Duplicated field
                        "name VARCHAR," +
                        "email VARCHAR," +
                        "age INTEGER," +
                        "department VARCHAR)"
        );

        // Record table (user_id only in primary key)
        client.sql().execute(null,
                "CREATE TABLE IF NOT EXISTS user_profiles_record (" +
                        "user_id VARCHAR PRIMARY KEY," +
                        "name VARCHAR," +
                        "email VARCHAR," +
                        "age INTEGER," +
                        "department VARCHAR)"
        );
    }

    private static void createProductCatalogTables(IgniteClient client) throws Exception {
        // Key-Value approach
        client.sql().execute(null,
                "CREATE TABLE IF NOT EXISTS products_kv (" +
                        "product_id VARCHAR PRIMARY KEY," +
                        "product_id_dup VARCHAR," +  // Duplicated field
                        "name VARCHAR," +
                        "category VARCHAR," +
                        "price DOUBLE," +
                        "inventory INTEGER)"
        );

        // Record approach
        client.sql().execute(null,
                "CREATE TABLE IF NOT EXISTS products_record (" +
                        "product_id VARCHAR PRIMARY KEY," +
                        "name VARCHAR," +
                        "category VARCHAR," +
                        "price DOUBLE," +
                        "inventory INTEGER)"
        );
    }

    private static void createPerformanceTables(IgniteClient client) throws Exception {
        // Key-Value performance test table
        client.sql().execute(null,
                "CREATE TABLE IF NOT EXISTS perf_test_kv (" +
                        "test_id VARCHAR PRIMARY KEY," +
                        "test_id_dup VARCHAR," +  // Duplicated field
                        "data1 VARCHAR," +
                        "data2 VARCHAR," +
                        "data3 INTEGER," +
                        "created_time BIGINT)"
        );

        // Record performance test table
        client.sql().execute(null,
                "CREATE TABLE IF NOT EXISTS perf_test_record (" +
                        "test_id VARCHAR PRIMARY KEY," +
                        "data1 VARCHAR," +
                        "data2 VARCHAR," +
                        "data3 INTEGER," +
                        "created_time BIGINT)"
        );
    }

    // User profile operations
    private static void storeUserWithKeyValue(KeyValueView<Tuple, Tuple> view, String userId, String name, String email, int age, String department) {
        Tuple key = Tuple.create().set("user_id", userId);
        Tuple value = Tuple.create()
                .set("user_id_dup", userId)  // Data duplication
                .set("name", name)
                .set("email", email)
                .set("age", age)
                .set("department", department);
        view.put(null, key, value);
        System.out.println("Stored user (KV): " + userId + " - " + name);
    }

    private static void storeUserWithRecord(RecordView<Tuple> view, String userId, String name, String email, int age, String department) {
        Tuple record = Tuple.create()
                .set("user_id", userId)  // No duplication
                .set("name", name)
                .set("email", email)
                .set("age", age)
                .set("department", department);
        view.insert(null, record);
        System.out.println("Stored user (Record): " + userId + " - " + name);
    }

    private static Tuple getUserWithKeyValue(KeyValueView<Tuple, Tuple> view, String userId) {
        Tuple key = Tuple.create().set("user_id", userId);
        Tuple value = view.get(null, key);
        if (value != null) {
            // Need to merge key and value to get complete record
            return Tuple.create()
                    .set("user_id", userId)
                    .set("name", value.stringValue("name"))
                    .set("email", value.stringValue("email"))
                    .set("age", value.intValue("age"))
                    .set("department", value.stringValue("department"));
        }
        return null;
    }

    private static Tuple getUserWithRecord(RecordView<Tuple> view, String userId) {
        Tuple key = Tuple.create().set("user_id", userId);
        return view.get(null, key);  // Complete record in single operation
    }

    private static void updateUserEmailKeyValue(KeyValueView<Tuple, Tuple> view, String userId, String newEmail) {
        Tuple key = Tuple.create().set("user_id", userId);
        Tuple currentValue = view.get(null, key);
        if (currentValue != null) {
            Tuple updatedValue = Tuple.create()
                    .set("user_id_dup", userId)  // Must maintain duplication
                    .set("name", currentValue.stringValue("name"))
                    .set("email", newEmail)
                    .set("age", currentValue.intValue("age"))
                    .set("department", currentValue.stringValue("department"));
            view.put(null, key, updatedValue);
        }
        System.out.println("Updated email (KV): " + userId + " -> " + newEmail);
    }

    private static void updateUserEmailRecord(RecordView<Tuple> view, String userId, String newEmail) {
        Tuple key = Tuple.create().set("user_id", userId);
        Tuple currentRecord = view.get(null, key);
        if (currentRecord != null) {
            Tuple updatedRecord = Tuple.create()
                    .set("user_id", userId)
                    .set("name", currentRecord.stringValue("name"))
                    .set("email", newEmail)  // Simple field update
                    .set("age", currentRecord.intValue("age"))
                    .set("department", currentRecord.stringValue("department"));
            view.replace(null, updatedRecord);
        }
        System.out.println("Updated email (Record): " + userId + " -> " + newEmail);
    }

    // Product catalog operations
    private static void storeProductsKeyValue(KeyValueView<Tuple, Tuple> view) {
        String[][] products = {
            {"PROD001", "Gaming Laptop", "Electronics", "1299.99", "50"},
            {"PROD002", "Wireless Mouse", "Electronics", "29.99", "200"},
            {"PROD003", "Office Chair", "Furniture", "249.99", "75"}
        };

        for (String[] product : products) {
            Tuple key = Tuple.create().set("product_id", product[0]);
            Tuple value = Tuple.create()
                    .set("product_id_dup", product[0])  // Duplication
                    .set("name", product[1])
                    .set("category", product[2])
                    .set("price", Double.parseDouble(product[3]))
                    .set("inventory", Integer.parseInt(product[4]));
            view.put(null, key, value);
        }
        System.out.println("Stored " + products.length + " products using Key-Value approach");
    }

    private static void storeProductsRecord(RecordView<Tuple> view) {
        String[][] products = {
            {"PROD001", "Gaming Laptop", "Electronics", "1299.99", "50"},
            {"PROD002", "Wireless Mouse", "Electronics", "29.99", "200"},
            {"PROD003", "Office Chair", "Furniture", "249.99", "75"}
        };

        for (String[] product : products) {
            Tuple record = Tuple.create()
                    .set("product_id", product[0])  // No duplication
                    .set("name", product[1])
                    .set("category", product[2])
                    .set("price", Double.parseDouble(product[3]))
                    .set("inventory", Integer.parseInt(product[4]));
            view.insert(null, record);
        }
        System.out.println("Stored " + products.length + " products using Record approach");
    }

    private static Tuple getProductWithKeyValue(KeyValueView<Tuple, Tuple> view, String productId) {
        Tuple key = Tuple.create().set("product_id", productId);
        Tuple value = view.get(null, key);
        if (value != null) {
            return Tuple.create()
                    .set("product_id", productId)
                    .set("name", value.stringValue("name"))
                    .set("category", value.stringValue("category"))
                    .set("price", value.doubleValue("price"))
                    .set("inventory", value.intValue("inventory"));
        }
        return null;
    }

    private static Tuple getProductWithRecord(RecordView<Tuple> view, String productId) {
        Tuple key = Tuple.create().set("product_id", productId);
        return view.get(null, key);
    }

    private static void updateProductPricesKeyValue(KeyValueView<Tuple, Tuple> view) {
        String[] productIds = {"PROD001", "PROD002", "PROD003"};
        double[] newPrices = {1199.99, 24.99, 199.99};

        for (int i = 0; i < productIds.length; i++) {
            Tuple key = Tuple.create().set("product_id", productIds[i]);
            Tuple currentValue = view.get(null, key);
            if (currentValue != null) {
                Tuple updatedValue = Tuple.create()
                        .set("product_id_dup", productIds[i])  // Maintain duplication
                        .set("name", currentValue.stringValue("name"))
                        .set("category", currentValue.stringValue("category"))
                        .set("price", newPrices[i])
                        .set("inventory", currentValue.intValue("inventory"));
                view.put(null, key, updatedValue);
            }
        }
        System.out.println("Updated prices for " + productIds.length + " products (Key-Value)");
    }

    private static void updateProductPricesRecord(RecordView<Tuple> view) {
        String[] productIds = {"PROD001", "PROD002", "PROD003"};
        double[] newPrices = {1199.99, 24.99, 199.99};

        for (int i = 0; i < productIds.length; i++) {
            Tuple key = Tuple.create().set("product_id", productIds[i]);
            Tuple currentRecord = view.get(null, key);
            if (currentRecord != null) {
                Tuple updatedRecord = Tuple.create()
                        .set("product_id", productIds[i])  // No duplication concerns
                        .set("name", currentRecord.stringValue("name"))
                        .set("category", currentRecord.stringValue("category"))
                        .set("price", newPrices[i])
                        .set("inventory", currentRecord.intValue("inventory"));
                view.replace(null, updatedRecord);
            }
        }
        System.out.println("Updated prices for " + productIds.length + " products (Record)");
    }

    // Performance testing methods
    private static void performKeyValueOperations(KeyValueView<Tuple, Tuple> view, int count) {
        for (int i = 0; i < count; i++) {
            String testId = "test_" + i;
            Tuple key = Tuple.create().set("test_id", testId);
            Tuple value = Tuple.create()
                    .set("test_id_dup", testId)  // Duplication overhead
                    .set("data1", "test_data_" + i)
                    .set("data2", "more_data_" + i)
                    .set("data3", i)
                    .set("created_time", System.currentTimeMillis());
            view.put(null, key, value);

            if (i % 2 == 0) {
                view.get(null, key);  // Read operation
            }
        }
    }

    private static void performRecordOperations(RecordView<Tuple> view, int count) {
        for (int i = 0; i < count; i++) {
            String testId = "test_" + i;
            Tuple record = Tuple.create()
                    .set("test_id", testId)  // No duplication
                    .set("data1", "test_data_" + i)
                    .set("data2", "more_data_" + i)
                    .set("data3", i)
                    .set("created_time", System.currentTimeMillis());
            view.insert(null, record);

            if (i % 2 == 0) {
                Tuple key = Tuple.create().set("test_id", testId);
                view.get(null, key);  // Read operation
            }
        }
    }

    private static void analyzeMemoryUsage() {
        System.out.println("Memory Usage Analysis:");
        System.out.println("- Key-Value approach duplicates primary key data in both key and value");
        System.out.println("- Record approach stores primary key only once per record");
        System.out.println("- For 1000 records with 10-character keys: ~10KB savings with Record View");
        System.out.println("- Record View also reduces application complexity and potential inconsistencies");
    }

    // Utility methods
    private static String formatUserTuple(Tuple user) {
        if (user == null) return "null";
        return String.format("User{id='%s', name='%s', email='%s', age=%d, dept='%s'}",
                user.stringValue("user_id"),
                user.stringValue("name"),
                user.stringValue("email"),
                user.intValue("age"),
                user.stringValue("department"));
    }

    private static String formatProductTuple(Tuple product) {
        if (product == null) return "null";
        return String.format("Product{id='%s', name='%s', category='%s', price=%.2f, inventory=%d}",
                product.stringValue("product_id"),
                product.stringValue("name"),
                product.stringValue("category"),
                product.doubleValue("price"),
                product.intValue("inventory"));
    }
}
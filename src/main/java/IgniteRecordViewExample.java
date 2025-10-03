import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Tuple;

/**
 * Demonstrates Record View API compared to Key-Value View API in Apache Ignite 3.
 *
 * Both APIs use the same underlying table structure. The difference is in API design:
 * KeyValueView separates key and value in method calls, while RecordView treats
 * the entire row as a single record. RecordView simplifies operations when the
 * primary key is part of the domain object, while KeyValueView is useful when
 * using surrogate keys or when key and value represent distinct domain concepts.
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
     * Both use the same table schema. KeyValueView separates key from value in API calls,
     * while RecordView uses a single record containing all fields.
     */
    private static void demonstrateUserProfileComparison(IgniteClient client) throws Exception {
        System.out.println("=== User Profile: Key-Value vs Record View Comparison ===");

        // Create table (same schema for both views)
        createUserProfileTable(client);

        // Get both views for the same table
        KeyValueView<Tuple, Tuple> kvView = client.tables()
                .table("user_profiles")
                .keyValueView();

        RecordView<Tuple> recordView = client.tables()
                .table("user_profiles")
                .recordView();

        // Store user data using Key-Value approach (separate key and value)
        System.out.println("--- Key-Value Approach (separate key and value parameters) ---");
        storeUserWithKeyValue(kvView, "alice", "Alice Johnson", "alice@example.com", 28, "Engineering");
        storeUserWithKeyValue(kvView, "bob", "Bob Smith", "bob@example.com", 34, "Marketing");

        // Store user data using Record View approach (single record)
        System.out.println("--- Record View Approach (single record parameter) ---");
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
     * Both use the same table schema. Demonstrates API differences in insert and update operations.
     */
    private static void demonstrateProductCatalogComparison(IgniteClient client) throws Exception {
        System.out.println("=== Product Catalog: Key-Value vs Record View Comparison ===");

        // Create table (same schema for both views)
        createProductCatalogTable(client);

        // Get both views for the same table
        KeyValueView<Tuple, Tuple> kvView = client.tables()
                .table("products")
                .keyValueView();

        RecordView<Tuple> recordView = client.tables()
                .table("products")
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
     * Demonstrates API ergonomics differences between KeyValueView and RecordView.
     * Both use the same table schema. RecordView simplifies code when working with complete records.
     */
    private static void demonstratePerformanceComparison(IgniteClient client) throws Exception {
        System.out.println("=== API Ergonomics Comparison ===");

        // Create table (same schema for both views)
        createPerformanceTable(client);

        // Get both views for the same table
        KeyValueView<Tuple, Tuple> kvView = client.tables()
                .table("perf_test")
                .keyValueView();

        RecordView<Tuple> recordView = client.tables()
                .table("perf_test")
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
    private static void createUserProfileTable(IgniteClient client) throws Exception {
        // Single table schema used by both KeyValueView and RecordView
        client.sql().execute(null, "DROP TABLE IF EXISTS user_profiles");
        client.sql().execute(null,
                "CREATE TABLE user_profiles (" +
                        "user_id VARCHAR PRIMARY KEY," +
                        "name VARCHAR," +
                        "email VARCHAR," +
                        "age INTEGER," +
                        "department VARCHAR)"
        );
    }

    private static void createProductCatalogTable(IgniteClient client) throws Exception {
        // Single table schema used by both KeyValueView and RecordView
        client.sql().execute(null, "DROP TABLE IF EXISTS products");
        client.sql().execute(null,
                "CREATE TABLE products (" +
                        "product_id VARCHAR PRIMARY KEY," +
                        "name VARCHAR," +
                        "category VARCHAR," +
                        "price DOUBLE," +
                        "inventory INTEGER)"
        );
    }

    private static void createPerformanceTable(IgniteClient client) throws Exception {
        // Single table schema used by both KeyValueView and RecordView
        client.sql().execute(null, "DROP TABLE IF EXISTS perf_test");
        client.sql().execute(null,
                "CREATE TABLE perf_test (" +
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
                .set("name", name)
                .set("email", email)
                .set("age", age)
                .set("department", department);
        view.put(null, key, value);
        System.out.println("Stored user (KV): " + userId + " - " + name);
    }

    private static void storeUserWithRecord(RecordView<Tuple> view, String userId, String name, String email, int age, String department) {
        Tuple record = Tuple.create()
                .set("user_id", userId)  // Primary key included in record
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
            // KeyValueView returns only the value, must manually merge with key to get complete record
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
        return view.get(null, key);  // RecordView returns complete record including primary key
    }

    private static void updateUserEmailKeyValue(KeyValueView<Tuple, Tuple> view, String userId, String newEmail) {
        Tuple key = Tuple.create().set("user_id", userId);
        Tuple currentValue = view.get(null, key);
        if (currentValue != null) {
            // KeyValueView requires rebuilding entire value Tuple for updates
            Tuple updatedValue = Tuple.create()
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
            // RecordView also requires full record for updates
            Tuple updatedRecord = Tuple.create()
                    .set("user_id", userId)
                    .set("name", currentRecord.stringValue("name"))
                    .set("email", newEmail)
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
                    .set("product_id", product[0])  // Primary key included in record
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
                        .set("product_id", productIds[i])  // Primary key included in record
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
                    .set("test_id", testId)  // Primary key included in record
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
        System.out.println("API Comparison Summary:");
        System.out.println("- Both KeyValueView and RecordView use identical table schemas");
        System.out.println("- KeyValueView: Separates key and value in API calls, value does not contain key fields");
        System.out.println("- RecordView: Uses single record containing all fields including primary key");
        System.out.println("- RecordView simplifies code when primary key is part of the domain object");
        System.out.println("- KeyValueView is useful when using surrogate keys or separate key/value concepts");
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
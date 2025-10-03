import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.mapper.Mapper;

/**
 * Demonstrates custom POJO key-value operations using Apache Ignite 3.
 *
 * Redis requires manual serialization of complex objects to strings or bytes,
 * typically using JSON or protocol buffers. Ignite handles POJO serialization
 * automatically using the Mapper system.
 *
 * This example shows how to use Mapper.builder() for explicit field-to-column
 * mapping when Java camelCase fields need to map to SQL snake_case columns.
 */
public class IgnitePojoExample {
    public static void main(String[] args) throws Exception {
        try (IgniteClient client = IgniteClient.builder()
                .addresses("127.0.0.1:10800")
                .build()) {

            demonstrateUserCache(client);
            demonstrateProductCache(client);
            demonstrateSessionCache(client);
        }
    }

    /**
     * Stores User objects with String keys for user profile caching.
     * Pattern: user_id -> User object
     * Uses Mapper.builder() for explicit field-to-column mapping.
     */
    private static void demonstrateUserCache(IgniteClient client) throws Exception {
        System.out.println("=== User Profile Cache (String Key, User Value) ===");

        // Drop and recreate table to ensure clean schema
        client.sql().execute(null, "DROP TABLE IF EXISTS user_profiles");
        client.sql().execute(null,
                "CREATE TABLE user_profiles (" +
                        "user_id VARCHAR PRIMARY KEY," +
                        "username VARCHAR," +
                        "email VARCHAR," +
                        "full_name VARCHAR," +
                        "age INT)"
        );

        // Use Mapper.builder() for explicit field-to-column mapping
        Mapper<User> userMapper = Mapper.builder(User.class)
                .automap()  // Auto-maps: username, email, age
                .map("fullName", "full_name")  // Explicit: fullName -> FULL_NAME
                .build();

        KeyValueView<String, User> userView = client.tables()
                .table("user_profiles")
                .keyValueView(Mapper.of(String.class), userMapper);

        // Create user objects
        User alice = new User("alice", "alice@example.com", "Alice Smith", 28);
        User bob = new User("bob", "bob@example.com", "Bob Johnson", 34);

        // Store users directly - no manual serialization needed
        userView.put(null, "user:alice", alice);
        userView.put(null, "user:bob", bob);

        // Retrieve users - automatic deserialization
        User retrievedAlice = userView.get(null, "user:alice");
        User retrievedBob = userView.get(null, "user:bob");

        System.out.println("Retrieved Alice: " + retrievedAlice);
        System.out.println("Retrieved Bob: " + retrievedBob);

        // Update user
        alice.setAge(29);
        userView.put(null, "user:alice", alice);
        User updatedAlice = userView.get(null, "user:alice");
        System.out.println("Updated Alice: " + updatedAlice);

        System.out.println();
    }

    /**
     * Stores Product objects with ProductKey for product catalog caching.
     * Pattern: ProductKey -> Product object
     * Uses Mapper.builder() with composite keys for explicit mapping.
     */
    private static void demonstrateProductCache(IgniteClient client) throws Exception {
        System.out.println("=== Product Catalog Cache (ProductKey, Product Value) ===");

        // Drop and recreate table to ensure clean schema
        client.sql().execute(null, "DROP TABLE IF EXISTS product_catalog");
        client.sql().execute(null,
                "CREATE TABLE product_catalog (" +
                        "category VARCHAR," +
                        "type VARCHAR," +
                        "sku VARCHAR," +
                        "name VARCHAR," +
                        "description VARCHAR," +
                        "price DOUBLE," +
                        "inventory_count INT," +
                        "PRIMARY KEY (category, type, sku))"
        );

        // Mapper for Product value with explicit field mapping
        Mapper<Product> productMapper = Mapper.builder(Product.class)
                .automap()
                .map("inventoryCount", "inventory_count")
                .build();

        KeyValueView<ProductKey, Product> productView = client.tables()
                .table("product_catalog")
                .keyValueView(Mapper.of(ProductKey.class), productMapper);

        // Create product objects with complex keys
        ProductKey laptopKey = new ProductKey("electronics", "laptop", "SKU123");
        Product laptop = new Product("Gaming Laptop", "Gaming laptop with advanced graphics", 1299.99, 50);

        ProductKey phoneKey = new ProductKey("electronics", "smartphone", "SKU456");
        Product phone = new Product("Smartphone", "Latest model smartphone", 799.99, 100);

        // Store products directly
        productView.put(null, laptopKey, laptop);
        productView.put(null, phoneKey, phone);

        // Retrieve products
        Product retrievedLaptop = productView.get(null, laptopKey);
        Product retrievedPhone = productView.get(null, phoneKey);

        System.out.println("Retrieved Laptop: " + retrievedLaptop);
        System.out.println("Retrieved Phone: " + retrievedPhone);

        // Update inventory
        laptop.setInventoryCount(laptop.getInventoryCount() - 1);
        productView.put(null, laptopKey, laptop);
        Product updatedLaptop = productView.get(null, laptopKey);
        System.out.println("Updated Laptop Inventory: " + updatedLaptop.getInventoryCount());

        System.out.println();
    }

    /**
     * Stores SessionData objects with SessionKey for session management.
     * Pattern: SessionKey -> SessionData object
     * Uses Mapper.builder() with composite keys for explicit mapping.
     */
    private static void demonstrateSessionCache(IgniteClient client) throws Exception {
        System.out.println("=== Session Management Cache (SessionKey, SessionData Value) ===");

        // Drop and recreate table to ensure clean schema
        client.sql().execute(null, "DROP TABLE IF EXISTS pojo_sessions");
        client.sql().execute(null,
                "CREATE TABLE pojo_sessions (" +
                        "user_id VARCHAR," +
                        "device_type VARCHAR," +
                        "ip_address VARCHAR," +
                        "role VARCHAR," +
                        "created_at BIGINT," +
                        "timeout_millis BIGINT," +
                        "PRIMARY KEY (user_id, device_type, ip_address))"
        );

        // Mapper for SessionKey with explicit field mapping
        Mapper<SessionKey> sessionKeyMapper = Mapper.builder(SessionKey.class)
                .automap()
                .map("userId", "user_id")
                .map("deviceType", "device_type")
                .map("ipAddress", "ip_address")
                .build();

        // Mapper for SessionData with explicit field mapping
        Mapper<SessionData> sessionDataMapper = Mapper.builder(SessionData.class)
                .automap()
                .map("createdAt", "created_at")
                .map("timeoutMillis", "timeout_millis")
                .build();

        KeyValueView<SessionKey, SessionData> sessionView = client.tables()
                .table("pojo_sessions")
                .keyValueView(sessionKeyMapper, sessionDataMapper);

        // Create session objects
        SessionKey aliceSessionKey = new SessionKey("alice", "web", "192.168.1.100");
        SessionData aliceSession = new SessionData("admin", System.currentTimeMillis(), 3600000);

        SessionKey bobSessionKey = new SessionKey("bob", "mobile", "10.0.1.50");
        SessionData bobSession = new SessionData("user", System.currentTimeMillis(), 1800000);

        // Store sessions directly
        sessionView.put(null, aliceSessionKey, aliceSession);
        sessionView.put(null, bobSessionKey, bobSession);

        // Retrieve sessions
        SessionData retrievedAliceSession = sessionView.get(null, aliceSessionKey);
        SessionData retrievedBobSession = sessionView.get(null, bobSessionKey);

        System.out.println("Alice Session: " + retrievedAliceSession);
        System.out.println("Bob Session: " + retrievedBobSession);

        // Check session validity
        boolean aliceSessionValid = isSessionValid(retrievedAliceSession);
        boolean bobSessionValid = isSessionValid(retrievedBobSession);
        System.out.println("Alice session valid: " + aliceSessionValid);
        System.out.println("Bob session valid: " + bobSessionValid);

        System.out.println();
    }

    private static boolean isSessionValid(SessionData session) {
        long currentTime = System.currentTimeMillis();
        return (currentTime - session.getCreatedAt()) < session.getTimeoutMillis();
    }

    // Domain objects for caching scenarios
    // POJOs require default no-arg constructor for Ignite Mapper
    static class User {
        private String username;
        private String email;
        private String fullName;
        private int age;

        public User() {} // Required for Ignite Mapper

        public User(String username, String email, String fullName, int age) {
            this.username = username;
            this.email = email;
            this.fullName = fullName;
            this.age = age;
        }

        public String getUsername() { return username; }
        public String getEmail() { return email; }
        public String getFullName() { return fullName; }
        public int getAge() { return age; }
        public void setAge(int age) { this.age = age; }

        @Override
        public String toString() {
            return "User{username='" + username + "', email='" + email +
                   "', fullName='" + fullName + "', age=" + age + "}";
        }
    }

    static class ProductKey {
        private String category;
        private String type;
        private String sku;

        public ProductKey() {} // Required for Ignite Mapper

        public ProductKey(String category, String type, String sku) {
            this.category = category;
            this.type = type;
            this.sku = sku;
        }

        public String getCategory() { return category; }
        public String getType() { return type; }
        public String getSku() { return sku; }

        @Override
        public String toString() {
            return "ProductKey{category='" + category + "', type='" + type + "', sku='" + sku + "'}";
        }
    }

    static class Product {
        private String name;
        private String description;
        private double price;
        private int inventoryCount;

        public Product() {} // Required for Ignite Mapper

        public Product(String name, String description, double price, int inventoryCount) {
            this.name = name;
            this.description = description;
            this.price = price;
            this.inventoryCount = inventoryCount;
        }

        public String getName() { return name; }
        public String getDescription() { return description; }
        public double getPrice() { return price; }
        public int getInventoryCount() { return inventoryCount; }
        public void setInventoryCount(int inventoryCount) { this.inventoryCount = inventoryCount; }

        @Override
        public String toString() {
            return "Product{name='" + name + "', description='" + description +
                   "', price=" + price + ", inventoryCount=" + inventoryCount + "}";
        }
    }

    static class SessionKey {
        private String userId;
        private String deviceType;
        private String ipAddress;

        public SessionKey() {} // Required for Ignite Mapper

        public SessionKey(String userId, String deviceType, String ipAddress) {
            this.userId = userId;
            this.deviceType = deviceType;
            this.ipAddress = ipAddress;
        }

        public String getUserId() { return userId; }
        public String getDeviceType() { return deviceType; }
        public String getIpAddress() { return ipAddress; }

        @Override
        public String toString() {
            return "SessionKey{userId='" + userId + "', deviceType='" + deviceType +
                   "', ipAddress='" + ipAddress + "'}";
        }
    }

    static class SessionData {
        private String role;
        private long createdAt;
        private long timeoutMillis;

        public SessionData() {} // Required for Ignite Mapper

        public SessionData(String role, long createdAt, long timeoutMillis) {
            this.role = role;
            this.createdAt = createdAt;
            this.timeoutMillis = timeoutMillis;
        }

        public String getRole() { return role; }
        public long getCreatedAt() { return createdAt; }
        public long getTimeoutMillis() { return timeoutMillis; }

        @Override
        public String toString() {
            return "SessionData{role='" + role +
                   "', createdAt=" + createdAt + ", timeoutMillis=" + timeoutMillis + "}";
        }
    }
}
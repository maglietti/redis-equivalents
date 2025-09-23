import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Tuple;

import java.io.Serializable;

/**
 * Demonstrates custom POJO key-value operations using Apache Ignite 3.
 *
 * Redis requires manual serialization of complex objects to strings or bytes,
 * typically using JSON or protocol buffers. Ignite handles POJO serialization
 * automatically using Java's built-in serialization mechanism.
 *
 * This pattern enables type-safe storage of domain objects without
 * manual serialization logic, common in session stores and caches.
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
     */
    private static void demonstrateUserCache(IgniteClient client) throws Exception {
        System.out.println("=== User Profile Cache (String Key, User Value) ===");

        client.sql().execute(null,
                "CREATE TABLE IF NOT EXISTS user_profiles (" +
                        "user_id VARCHAR PRIMARY KEY," +
                        "user_data VARBINARY)"
        );

        KeyValueView<Tuple, Tuple> userView = client.tables()
                .table("user_profiles")
                .keyValueView();

        // Create user objects
        User alice = new User("alice", "alice@example.com", "Alice Smith", 28);
        User bob = new User("bob", "bob@example.com", "Bob Johnson", 34);

        // Store users using string keys
        storeUser(userView, "user:alice", alice);
        storeUser(userView, "user:bob", bob);

        // Retrieve users
        User retrievedAlice = getUser(userView, "user:alice");
        User retrievedBob = getUser(userView, "user:bob");

        System.out.println("Retrieved Alice: " + retrievedAlice);
        System.out.println("Retrieved Bob: " + retrievedBob);

        // Update user
        alice.setAge(29);
        storeUser(userView, "user:alice", alice);
        User updatedAlice = getUser(userView, "user:alice");
        System.out.println("Updated Alice: " + updatedAlice);

        System.out.println();
    }

    /**
     * Stores Product objects with ProductKey for product catalog caching.
     * Pattern: ProductKey -> Product object
     */
    private static void demonstrateProductCache(IgniteClient client) throws Exception {
        System.out.println("=== Product Catalog Cache (ProductKey, Product Value) ===");

        client.sql().execute(null,
                "CREATE TABLE IF NOT EXISTS product_catalog (" +
                        "product_key VARBINARY PRIMARY KEY," +
                        "product_data VARBINARY)"
        );

        KeyValueView<Tuple, Tuple> productView = client.tables()
                .table("product_catalog")
                .keyValueView();

        // Create product objects with complex keys
        ProductKey laptopKey = new ProductKey("electronics", "laptop", "SKU123");
        Product laptop = new Product("Gaming Laptop", "Gaming laptop with advanced graphics", 1299.99, 50);

        ProductKey phoneKey = new ProductKey("electronics", "smartphone", "SKU456");
        Product phone = new Product("Smartphone", "Latest model smartphone", 799.99, 100);

        // Store products
        storeProduct(productView, laptopKey, laptop);
        storeProduct(productView, phoneKey, phone);

        // Retrieve products
        Product retrievedLaptop = getProduct(productView, laptopKey);
        Product retrievedPhone = getProduct(productView, phoneKey);

        System.out.println("Retrieved Laptop: " + retrievedLaptop);
        System.out.println("Retrieved Phone: " + retrievedPhone);

        // Update inventory
        laptop.setInventoryCount(laptop.getInventoryCount() - 1);
        storeProduct(productView, laptopKey, laptop);
        Product updatedLaptop = getProduct(productView, laptopKey);
        System.out.println("Updated Laptop Inventory: " + updatedLaptop.getInventoryCount());

        System.out.println();
    }

    /**
     * Stores SessionData objects with SessionKey for session management.
     * Pattern: SessionKey -> SessionData object
     */
    private static void demonstrateSessionCache(IgniteClient client) throws Exception {
        System.out.println("=== Session Management Cache (SessionKey, SessionData Value) ===");

        client.sql().execute(null,
                "CREATE TABLE IF NOT EXISTS pojo_sessions (" +
                        "session_key VARBINARY PRIMARY KEY," +
                        "session_data VARBINARY)"
        );

        KeyValueView<Tuple, Tuple> sessionView = client.tables()
                .table("pojo_sessions")
                .keyValueView();

        // Create session objects
        SessionKey aliceSessionKey = new SessionKey("alice", "web", "192.168.1.100");
        SessionData aliceSession = new SessionData("alice", "admin", System.currentTimeMillis(), 3600000);

        SessionKey bobSessionKey = new SessionKey("bob", "mobile", "10.0.1.50");
        SessionData bobSession = new SessionData("bob", "user", System.currentTimeMillis(), 1800000);

        // Store sessions
        storeSession(sessionView, aliceSessionKey, aliceSession);
        storeSession(sessionView, bobSessionKey, bobSession);

        // Retrieve sessions
        SessionData retrievedAliceSession = getSession(sessionView, aliceSessionKey);
        SessionData retrievedBobSession = getSession(sessionView, bobSessionKey);

        System.out.println("Alice Session: " + retrievedAliceSession);
        System.out.println("Bob Session: " + retrievedBobSession);

        // Check session validity
        boolean aliceSessionValid = isSessionValid(retrievedAliceSession);
        boolean bobSessionValid = isSessionValid(retrievedBobSession);
        System.out.println("Alice session valid: " + aliceSessionValid);
        System.out.println("Bob session valid: " + bobSessionValid);

        System.out.println();
    }

    // Helper methods for User operations
    private static void storeUser(KeyValueView<Tuple, Tuple> view, String userId, User user) {
        Tuple key = Tuple.create().set("user_id", userId);
        Tuple value = Tuple.create().set("user_data", serializeObject(user));
        view.put(null, key, value);
    }

    private static User getUser(KeyValueView<Tuple, Tuple> view, String userId) {
        Tuple key = Tuple.create().set("user_id", userId);
        Tuple value = view.get(null, key);
        if (value != null) {
            byte[] data = value.value("user_data");
            return (User) deserializeObject(data);
        }
        return null;
    }

    // Helper methods for Product operations
    private static void storeProduct(KeyValueView<Tuple, Tuple> view, ProductKey productKey, Product product) {
        Tuple key = Tuple.create().set("product_key", serializeObject(productKey));
        Tuple value = Tuple.create().set("product_data", serializeObject(product));
        view.put(null, key, value);
    }

    private static Product getProduct(KeyValueView<Tuple, Tuple> view, ProductKey productKey) {
        Tuple key = Tuple.create().set("product_key", serializeObject(productKey));
        Tuple value = view.get(null, key);
        if (value != null) {
            byte[] data = value.value("product_data");
            return (Product) deserializeObject(data);
        }
        return null;
    }

    // Helper methods for Session operations
    private static void storeSession(KeyValueView<Tuple, Tuple> view, SessionKey sessionKey, SessionData sessionData) {
        Tuple key = Tuple.create().set("session_key", serializeObject(sessionKey));
        Tuple value = Tuple.create().set("session_data", serializeObject(sessionData));
        view.put(null, key, value);
    }

    private static SessionData getSession(KeyValueView<Tuple, Tuple> view, SessionKey sessionKey) {
        Tuple key = Tuple.create().set("session_key", serializeObject(sessionKey));
        Tuple value = view.get(null, key);
        if (value != null) {
            byte[] data = value.value("session_data");
            return (SessionData) deserializeObject(data);
        }
        return null;
    }

    private static boolean isSessionValid(SessionData session) {
        long currentTime = System.currentTimeMillis();
        return (currentTime - session.getCreatedAt()) < session.getTimeoutMillis();
    }

    // Serialization helpers
    private static byte[] serializeObject(Serializable obj) {
        try (java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
             java.io.ObjectOutputStream oos = new java.io.ObjectOutputStream(baos)) {
            oos.writeObject(obj);
            return baos.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException("Serialization failed", e);
        }
    }

    private static Object deserializeObject(byte[] data) {
        try (java.io.ByteArrayInputStream bais = new java.io.ByteArrayInputStream(data);
             java.io.ObjectInputStream ois = new java.io.ObjectInputStream(bais)) {
            return ois.readObject();
        } catch (Exception e) {
            throw new RuntimeException("Deserialization failed", e);
        }
    }

    // Domain objects for caching scenarios
    static class User implements Serializable {
        private String username;
        private String email;
        private String fullName;
        private int age;

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

    static class ProductKey implements Serializable {
        private String category;
        private String type;
        private String sku;

        public ProductKey(String category, String type, String sku) {
            this.category = category;
            this.type = type;
            this.sku = sku;
        }

        @Override
        public String toString() {
            return "ProductKey{category='" + category + "', type='" + type + "', sku='" + sku + "'}";
        }
    }

    static class Product implements Serializable {
        private String name;
        private String description;
        private double price;
        private int inventoryCount;

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

    static class SessionKey implements Serializable {
        private String userId;
        private String deviceType;
        private String ipAddress;

        public SessionKey(String userId, String deviceType, String ipAddress) {
            this.userId = userId;
            this.deviceType = deviceType;
            this.ipAddress = ipAddress;
        }

        @Override
        public String toString() {
            return "SessionKey{userId='" + userId + "', deviceType='" + deviceType +
                   "', ipAddress='" + ipAddress + "'}";
        }
    }

    static class SessionData implements Serializable {
        private String userId;
        private String role;
        private long createdAt;
        private long timeoutMillis;

        public SessionData(String userId, String role, long createdAt, long timeoutMillis) {
            this.userId = userId;
            this.role = role;
            this.createdAt = createdAt;
            this.timeoutMillis = timeoutMillis;
        }

        public String getUserId() { return userId; }
        public String getRole() { return role; }
        public long getCreatedAt() { return createdAt; }
        public long getTimeoutMillis() { return timeoutMillis; }

        @Override
        public String toString() {
            return "SessionData{userId='" + userId + "', role='" + role +
                   "', createdAt=" + createdAt + ", timeoutMillis=" + timeoutMillis + "}";
        }
    }
}
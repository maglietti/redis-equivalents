import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Tuple;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * Demonstrates serialized key-value operations using Apache Ignite 3.
 *
 * Redis treats all data as byte arrays requiring external serialization,
 * while Ignite supports both automatic POJO serialization and manual
 * control over serialization formats like JSON and Java serialization.
 *
 * This pattern is essential for Kafka integration where messages arrive
 * pre-serialized and applications need direct access to serialized data.
 */
public class IgniteSerializationExample {
    public static void main(String[] args) throws Exception {
        try (IgniteClient client = IgniteClient.builder()
                .addresses("127.0.0.1:10800")
                .build()) {

            demonstrateJsonSerialization(client);
            demonstrateJavaSerialization(client);
            demonstrateKafkaMessagePattern(client);
        }
    }

    /**
     * Stores objects as JSON strings for cross-language compatibility.
     * Pattern: JSON serialization for API responses and cross-system data.
     */
    private static void demonstrateJsonSerialization(IgniteClient client) throws Exception {
        System.out.println("=== JSON Serialization Example ===");

        client.sql().execute(null,
                "CREATE TABLE IF NOT EXISTS json_cache (" +
                        "key VARCHAR PRIMARY KEY," +
                        "json_data VARCHAR)"
        );

        KeyValueView<Tuple, Tuple> jsonView = client.tables()
                .table("json_cache")
                .keyValueView();

        // Store user profile as JSON
        UserProfile profile = new UserProfile("alice", "Alice Johnson", "alice@example.com", 28, "admin");
        String profileJson = toJson(profile);
        storeJson(jsonView, "user:alice", profileJson);

        // Store API response as JSON
        ApiResponse response = new ApiResponse("success", 200, "User retrieved successfully", System.currentTimeMillis());
        String responseJson = toJson(response);
        storeJson(jsonView, "api:response:123", responseJson);

        // Retrieve and deserialize JSON data
        String retrievedProfileJson = getJson(jsonView, "user:alice");
        UserProfile retrievedProfile = fromJson(retrievedProfileJson, UserProfile.class);
        System.out.println("Retrieved user profile: " + retrievedProfile);

        String retrievedResponseJson = getJson(jsonView, "api:response:123");
        ApiResponse retrievedResponse = fromJson(retrievedResponseJson, ApiResponse.class);
        System.out.println("Retrieved API response: " + retrievedResponse);

        // Demonstrate JSON field access without full deserialization
        String userName = extractJsonField(retrievedProfileJson, "name");
        String responseStatus = extractJsonField(retrievedResponseJson, "status");
        System.out.println("User name from JSON: " + userName);
        System.out.println("Response status from JSON: " + responseStatus);

        System.out.println();
    }

    /**
     * Stores objects using Java's built-in serialization.
     * Pattern: Binary serialization for Java-specific applications.
     */
    private static void demonstrateJavaSerialization(IgniteClient client) throws Exception {
        System.out.println("=== Java Serialization Example ===");

        client.sql().execute(null,
                "CREATE TABLE IF NOT EXISTS binary_cache (" +
                        "key VARCHAR PRIMARY KEY," +
                        "binary_data VARBINARY)"
        );

        KeyValueView<Tuple, Tuple> binaryView = client.tables()
                .table("binary_cache")
                .keyValueView();

        // Store complex objects using Java serialization
        ShoppingCart cart = new ShoppingCart("user_123");
        cart.addItem("laptop", 1299.99, 1);
        cart.addItem("mouse", 29.99, 2);
        cart.addItem("keyboard", 79.99, 1);

        storeBinary(binaryView, "cart:user_123", cart);

        // Store configuration object
        AppConfig config = new AppConfig();
        config.setMaxConnections(100);
        config.setTimeoutMs(5000);
        config.setRetryAttempts(3);
        config.enableFeature("dark_mode");
        config.enableFeature("notifications");

        storeBinary(binaryView, "config:app", config);

        // Retrieve and deserialize binary data
        ShoppingCart retrievedCart = (ShoppingCart) getBinary(binaryView, "cart:user_123");
        System.out.println("Retrieved shopping cart: " + retrievedCart);

        AppConfig retrievedConfig = (AppConfig) getBinary(binaryView, "config:app");
        System.out.println("Retrieved app config: " + retrievedConfig);

        // Modify and store updated objects
        retrievedCart.addItem("monitor", 299.99, 1);
        storeBinary(binaryView, "cart:user_123", retrievedCart);

        retrievedConfig.setTimeoutMs(10000);
        storeBinary(binaryView, "config:app", retrievedConfig);

        System.out.println();
    }

    /**
     * Simulates Kafka message processing with pre-serialized data.
     * Pattern: Process serialized messages without unnecessary deserialization.
     */
    private static void demonstrateKafkaMessagePattern(IgniteClient client) throws Exception {
        System.out.println("=== Kafka Message Processing Pattern ===");

        client.sql().execute(null,
                "CREATE TABLE IF NOT EXISTS kafka_messages (" +
                        "topic VARCHAR," +
                        "partition_key VARCHAR," +
                        "message_data VARBINARY," +
                        "metadata VARCHAR," +
                        "PRIMARY KEY (topic, partition_key))"
        );

        KeyValueView<Tuple, Tuple> messageView = client.tables()
                .table("kafka_messages")
                .keyValueView();

        // Simulate processing Kafka messages
        processKafkaMessage(messageView, "user-events", "user_001",
                           createUserEvent("user_001", "LOGIN", System.currentTimeMillis()));

        processKafkaMessage(messageView, "user-events", "user_002",
                           createUserEvent("user_002", "PURCHASE", System.currentTimeMillis()));

        processKafkaMessage(messageView, "orders", "order_501",
                           createOrderEvent("order_501", "user_001", 1299.99, "CREATED"));

        // Retrieve messages for processing
        KafkaMessage userMessage = getKafkaMessage(messageView, "user-events", "user_001");
        KafkaMessage orderMessage = getKafkaMessage(messageView, "orders", "order_501");

        System.out.println("User event message: " + userMessage);
        System.out.println("Order event message: " + orderMessage);

        // Process message without full deserialization
        processUserEventMessage(userMessage);
        processOrderEventMessage(orderMessage);

        System.out.println();
    }

    // JSON serialization helpers
    private static void storeJson(KeyValueView<Tuple, Tuple> view, String key, String jsonData) {
        Tuple keyTuple = Tuple.create().set("key", key);
        Tuple valueTuple = Tuple.create().set("json_data", jsonData);
        view.put(null, keyTuple, valueTuple);
    }

    private static String getJson(KeyValueView<Tuple, Tuple> view, String key) {
        Tuple keyTuple = Tuple.create().set("key", key);
        Tuple value = view.get(null, keyTuple);
        return value != null ? value.stringValue("json_data") : null;
    }

    // Binary serialization helpers
    private static void storeBinary(KeyValueView<Tuple, Tuple> view, String key, Serializable object) {
        Tuple keyTuple = Tuple.create().set("key", key);
        Tuple valueTuple = Tuple.create().set("binary_data", serializeObject(object));
        view.put(null, keyTuple, valueTuple);
    }

    private static Object getBinary(KeyValueView<Tuple, Tuple> view, String key) {
        Tuple keyTuple = Tuple.create().set("key", key);
        Tuple value = view.get(null, keyTuple);
        if (value != null) {
            byte[] data = value.value("binary_data");
            return deserializeObject(data);
        }
        return null;
    }

    // Kafka message helpers
    private static void processKafkaMessage(KeyValueView<Tuple, Tuple> view, String topic, String partitionKey, KafkaMessage message) {
        Tuple key = Tuple.create()
                .set("topic", topic)
                .set("partition_key", partitionKey);
        Tuple value = Tuple.create()
                .set("message_data", serializeObject(message))
                .set("metadata", message.getMetadata());
        view.put(null, key, value);
    }

    private static KafkaMessage getKafkaMessage(KeyValueView<Tuple, Tuple> view, String topic, String partitionKey) {
        Tuple key = Tuple.create()
                .set("topic", topic)
                .set("partition_key", partitionKey);
        Tuple value = view.get(null, key);
        if (value != null) {
            byte[] data = value.value("message_data");
            return (KafkaMessage) deserializeObject(data);
        }
        return null;
    }

    // Serialization utilities
    private static byte[] serializeObject(Serializable obj) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(obj);
            return baos.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException("Serialization failed", e);
        }
    }

    private static Object deserializeObject(byte[] data) {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(data);
             ObjectInputStream ois = new ObjectInputStream(bais)) {
            return ois.readObject();
        } catch (Exception e) {
            throw new RuntimeException("Deserialization failed", e);
        }
    }

    // JSON serialization for demonstration purposes
    private static String toJson(Object obj) {
        if (obj instanceof UserProfile) {
            UserProfile p = (UserProfile) obj;
            return String.format("{\"username\":\"%s\",\"name\":\"%s\",\"email\":\"%s\",\"age\":%d,\"role\":\"%s\"}",
                    p.username, p.name, p.email, p.age, p.role);
        } else if (obj instanceof ApiResponse) {
            ApiResponse r = (ApiResponse) obj;
            return String.format("{\"status\":\"%s\",\"code\":%d,\"message\":\"%s\",\"timestamp\":%d}",
                    r.status, r.code, r.message, r.timestamp);
        }
        return "{}";
    }

    @SuppressWarnings("unchecked")
    private static <T> T fromJson(String json, Class<T> clazz) {
        if (clazz == UserProfile.class) {
            Map<String, String> fields = parseJsonFields(json);
            return (T) new UserProfile(
                    fields.get("username"), fields.get("name"), fields.get("email"),
                    Integer.parseInt(fields.get("age")), fields.get("role"));
        } else if (clazz == ApiResponse.class) {
            Map<String, String> fields = parseJsonFields(json);
            return (T) new ApiResponse(
                    fields.get("status"), Integer.parseInt(fields.get("code")),
                    fields.get("message"), Long.parseLong(fields.get("timestamp")));
        }
        return null;
    }

    private static String extractJsonField(String json, String fieldName) {
        Map<String, String> fields = parseJsonFields(json);
        return fields.get(fieldName);
    }

    private static Map<String, String> parseJsonFields(String json) {
        Map<String, String> fields = new HashMap<>();
        String content = json.substring(1, json.length() - 1); // Remove { }
        String[] pairs = content.split(",");
        for (String pair : pairs) {
            String[] keyValue = pair.split(":");
            String key = keyValue[0].trim().replaceAll("\"", "");
            String value = keyValue[1].trim().replaceAll("\"", "");
            fields.put(key, value);
        }
        return fields;
    }

    // Kafka message processing
    private static void processUserEventMessage(KafkaMessage message) {
        System.out.println("Processing user event: " + message.getEventType() + " for user " + message.getUserId());
    }

    private static void processOrderEventMessage(KafkaMessage message) {
        System.out.println("Processing order event: " + message.getEventType() + " for order " + message.getOrderId());
    }

    private static KafkaMessage createUserEvent(String userId, String eventType, long timestamp) {
        return new KafkaMessage(userId, null, eventType, timestamp, "user-event:" + userId + ":" + timestamp);
    }

    private static KafkaMessage createOrderEvent(String orderId, String userId, double amount, String eventType) {
        return new KafkaMessage(userId, orderId, eventType, System.currentTimeMillis(), "order-event:" + orderId);
    }

    // Domain objects for serialization examples
    static class UserProfile {
        String username;
        String name;
        String email;
        int age;
        String role;

        public UserProfile(String username, String name, String email, int age, String role) {
            this.username = username;
            this.name = name;
            this.email = email;
            this.age = age;
            this.role = role;
        }

        @Override
        public String toString() {
            return "UserProfile{username='" + username + "', name='" + name +
                   "', email='" + email + "', age=" + age + ", role='" + role + "'}";
        }
    }

    static class ApiResponse {
        String status;
        int code;
        String message;
        long timestamp;

        public ApiResponse(String status, int code, String message, long timestamp) {
            this.status = status;
            this.code = code;
            this.message = message;
            this.timestamp = timestamp;
        }

        @Override
        public String toString() {
            return "ApiResponse{status='" + status + "', code=" + code +
                   ", message='" + message + "', timestamp=" + timestamp + "}";
        }
    }

    static class ShoppingCart implements Serializable {
        private String userId;
        private Map<String, CartItem> items;
        private double totalAmount;

        public ShoppingCart(String userId) {
            this.userId = userId;
            this.items = new HashMap<>();
            this.totalAmount = 0.0;
        }

        public void addItem(String productName, double price, int quantity) {
            items.put(productName, new CartItem(productName, price, quantity));
            calculateTotal();
        }

        private void calculateTotal() {
            totalAmount = items.values().stream()
                    .mapToDouble(item -> item.price * item.quantity)
                    .sum();
        }

        @Override
        public String toString() {
            return "ShoppingCart{userId='" + userId + "', items=" + items.size() +
                   ", totalAmount=" + totalAmount + "}";
        }

        static class CartItem implements Serializable {
            String productName;
            double price;
            int quantity;

            public CartItem(String productName, double price, int quantity) {
                this.productName = productName;
                this.price = price;
                this.quantity = quantity;
            }
        }
    }

    static class AppConfig implements Serializable {
        private int maxConnections;
        private long timeoutMs;
        private int retryAttempts;
        private Map<String, Boolean> features;

        public AppConfig() {
            this.features = new HashMap<>();
        }

        public void setMaxConnections(int maxConnections) { this.maxConnections = maxConnections; }
        public void setTimeoutMs(long timeoutMs) { this.timeoutMs = timeoutMs; }
        public void setRetryAttempts(int retryAttempts) { this.retryAttempts = retryAttempts; }
        public void enableFeature(String feature) { features.put(feature, true); }

        @Override
        public String toString() {
            return "AppConfig{maxConnections=" + maxConnections + ", timeoutMs=" + timeoutMs +
                   ", retryAttempts=" + retryAttempts + ", features=" + features + "}";
        }
    }

    static class KafkaMessage implements Serializable {
        private String userId;
        private String orderId;
        private String eventType;
        private long timestamp;
        private String metadata;

        public KafkaMessage(String userId, String orderId, String eventType, long timestamp, String metadata) {
            this.userId = userId;
            this.orderId = orderId;
            this.eventType = eventType;
            this.timestamp = timestamp;
            this.metadata = metadata;
        }

        public String getUserId() { return userId; }
        public String getOrderId() { return orderId; }
        public String getEventType() { return eventType; }
        public String getMetadata() { return metadata; }

        @Override
        public String toString() {
            return "KafkaMessage{userId='" + userId + "', orderId='" + orderId +
                   "', eventType='" + eventType + "', timestamp=" + timestamp + "}";
        }
    }
}
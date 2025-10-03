import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.Mapper;

import java.util.HashMap;
import java.util.Map;

/**
 * Demonstrates serialization patterns using Apache Ignite 3.
 *
 * Redis treats all data as byte arrays requiring external serialization,
 * while Ignite uses the Mapper system for automatic POJO serialization.
 * This example shows when to use Mappers (normal POJO storage) versus
 * when manual JSON serialization is appropriate (cross-language compatibility).
 *
 * For most use cases, including Kafka integration with POJOs, use Mappers
 * to let Ignite handle serialization automatically.
 */
public class IgniteSerializationExample {
    public static void main(String[] args) throws Exception {
        try (IgniteClient client = IgniteClient.builder()
                .addresses("127.0.0.1:10800")
                .build()) {

            demonstrateJsonSerialization(client);
            demonstratePojoWithMappers(client);
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
     * Stores POJOs using Ignite's Mapper system for automatic serialization.
     * Pattern: Type-safe POJO storage for Java applications.
     * This is the recommended approach for storing complex objects.
     */
    private static void demonstratePojoWithMappers(IgniteClient client) throws Exception {
        System.out.println("=== POJO Storage with Mappers ===");

        // Drop and recreate tables for clean schema
        client.sql().execute(null, "DROP TABLE IF EXISTS shopping_carts");
        client.sql().execute(null,
                "CREATE TABLE shopping_carts (" +
                        "cart_id VARCHAR PRIMARY KEY," +
                        "user_id VARCHAR," +
                        "total_amount DOUBLE," +
                        "item_count INT)"
        );

        client.sql().execute(null, "DROP TABLE IF EXISTS app_configs");
        client.sql().execute(null,
                "CREATE TABLE app_configs (" +
                        "config_key VARCHAR PRIMARY KEY," +
                        "max_connections INT," +
                        "timeout_ms BIGINT," +
                        "retry_attempts INT," +
                        "feature_flags VARCHAR)"
        );

        // Mapper for ShoppingCart with explicit field mapping
        // Note: cart_id is the key, so it's NOT in the value mapper
        Mapper<ShoppingCart> cartMapper = Mapper.builder(ShoppingCart.class)
                .automap()
                .map("userId", "user_id")
                .map("totalAmount", "total_amount")
                .map("itemCount", "item_count")
                .build();

        // Mapper for AppConfig with explicit field mapping
        // Note: config_key is the key, so it's NOT in the value mapper
        Mapper<AppConfig> configMapper = Mapper.builder(AppConfig.class)
                .automap()
                .map("maxConnections", "max_connections")
                .map("timeoutMs", "timeout_ms")
                .map("retryAttempts", "retry_attempts")
                .map("featureFlags", "feature_flags")
                .build();

        KeyValueView<String, ShoppingCart> cartView = client.tables()
                .table("shopping_carts")
                .keyValueView(Mapper.of(String.class), cartMapper);

        KeyValueView<String, AppConfig> configView = client.tables()
                .table("app_configs")
                .keyValueView(Mapper.of(String.class), configMapper);

        // Store complex objects directly - Mapper handles serialization
        ShoppingCart cart = new ShoppingCart("user_123");
        cart.addItem("laptop", 1299.99, 1);
        cart.addItem("mouse", 29.99, 2);
        cart.addItem("keyboard", 79.99, 1);
        cartView.put(null, "cart:user_123", cart);

        // Store configuration object
        AppConfig config = new AppConfig();
        config.setMaxConnections(100);
        config.setTimeoutMs(5000);
        config.setRetryAttempts(3);
        config.enableFeature("dark_mode");
        config.enableFeature("notifications");
        configView.put(null, "config:app", config);

        // Retrieve objects - automatic deserialization
        ShoppingCart retrievedCart = cartView.get(null, "cart:user_123");
        System.out.println("Retrieved shopping cart: " + retrievedCart);

        AppConfig retrievedConfig = configView.get(null, "config:app");
        System.out.println("Retrieved app config: " + retrievedConfig);

        // Modify and store updated objects
        retrievedCart.addItem("monitor", 299.99, 1);
        cartView.put(null, "cart:user_123", retrievedCart);

        retrievedConfig.setTimeoutMs(10000);
        configView.put(null, "config:app", retrievedConfig);

        System.out.println("Updated shopping cart: " + cartView.get(null, "cart:user_123"));
        System.out.println("Updated app config: " + configView.get(null, "config:app"));

        System.out.println();
    }

    /**
     * Simulates Kafka message processing using Mappers.
     * Pattern: Store deserialized Kafka messages as POJOs using Mappers.
     * When receiving messages from Kafka, deserialize once and store as POJOs.
     */
    private static void demonstrateKafkaMessagePattern(IgniteClient client) throws Exception {
        System.out.println("=== Kafka Message Processing Pattern ===");

        // Drop and recreate table for clean schema
        client.sql().execute(null, "DROP TABLE IF EXISTS kafka_messages");
        client.sql().execute(null,
                "CREATE TABLE kafka_messages (" +
                        "topic VARCHAR," +
                        "partition_key VARCHAR," +
                        "user_id VARCHAR," +
                        "order_id VARCHAR," +
                        "event_type VARCHAR," +
                        "event_timestamp BIGINT," +
                        "metadata VARCHAR," +
                        "PRIMARY KEY (topic, partition_key))"
        );

        // Mapper for KafkaMessageKey with explicit field mapping
        Mapper<KafkaMessageKey> keyMapper = Mapper.builder(KafkaMessageKey.class)
                .automap()
                .map("partitionKey", "partition_key")
                .build();

        // Mapper for KafkaMessage with explicit field mapping
        Mapper<KafkaMessage> messageMapper = Mapper.builder(KafkaMessage.class)
                .automap()
                .map("userId", "user_id")
                .map("orderId", "order_id")
                .map("eventType", "event_type")
                .map("eventTimestamp", "event_timestamp")
                .build();

        KeyValueView<KafkaMessageKey, KafkaMessage> messageView = client.tables()
                .table("kafka_messages")
                .keyValueView(keyMapper, messageMapper);

        // Simulate processing Kafka messages - store POJOs directly
        KafkaMessage userEvent1 = createUserEvent("user_001", "LOGIN", System.currentTimeMillis());
        messageView.put(null, new KafkaMessageKey("user-events", "user_001"), userEvent1);

        KafkaMessage userEvent2 = createUserEvent("user_002", "PURCHASE", System.currentTimeMillis());
        messageView.put(null, new KafkaMessageKey("user-events", "user_002"), userEvent2);

        KafkaMessage orderEvent = createOrderEvent("order_501", "user_001", 1299.99, "CREATED");
        messageView.put(null, new KafkaMessageKey("orders", "order_501"), orderEvent);

        // Retrieve messages for processing - automatic deserialization
        KafkaMessage userMessage = messageView.get(null, new KafkaMessageKey("user-events", "user_001"));
        KafkaMessage orderMessage = messageView.get(null, new KafkaMessageKey("orders", "order_501"));

        System.out.println("User event message: " + userMessage);
        System.out.println("Order event message: " + orderMessage);

        // Process messages
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

    // Composite key for Kafka messages
    static class KafkaMessageKey {
        private String topic;
        private String partitionKey;

        public KafkaMessageKey() {} // Required for Ignite Mapper

        public KafkaMessageKey(String topic, String partitionKey) {
            this.topic = topic;
            this.partitionKey = partitionKey;
        }

        public String getTopic() { return topic; }
        public String getPartitionKey() { return partitionKey; }
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

    static class ShoppingCart {
        private String userId;
        private double totalAmount;
        private int itemCount;

        public ShoppingCart() {} // Required for Ignite Mapper

        public ShoppingCart(String userId) {
            this.userId = userId;
            this.totalAmount = 0.0;
            this.itemCount = 0;
        }

        public void addItem(String productName, double price, int quantity) {
            this.totalAmount += price * quantity;
            this.itemCount += quantity;
        }

        public String getUserId() { return userId; }
        public double getTotalAmount() { return totalAmount; }
        public int getItemCount() { return itemCount; }

        @Override
        public String toString() {
            return "ShoppingCart{userId='" + userId +
                   "', itemCount=" + itemCount + ", totalAmount=" + totalAmount + "}";
        }
    }

    static class AppConfig {
        private int maxConnections;
        private long timeoutMs;
        private int retryAttempts;
        private String featureFlags; // Stored as comma-separated string for simplicity

        public AppConfig() {
            this.featureFlags = "";
        }

        public void setMaxConnections(int maxConnections) { this.maxConnections = maxConnections; }
        public void setTimeoutMs(long timeoutMs) { this.timeoutMs = timeoutMs; }
        public void setRetryAttempts(int retryAttempts) { this.retryAttempts = retryAttempts; }
        public void enableFeature(String feature) {
            if (featureFlags.isEmpty()) {
                featureFlags = feature;
            } else {
                featureFlags += "," + feature;
            }
        }

        public int getMaxConnections() { return maxConnections; }
        public long getTimeoutMs() { return timeoutMs; }
        public int getRetryAttempts() { return retryAttempts; }
        public String getFeatureFlags() { return featureFlags; }

        @Override
        public String toString() {
            return "AppConfig{maxConnections=" + maxConnections +
                   ", timeoutMs=" + timeoutMs + ", retryAttempts=" + retryAttempts +
                   ", featureFlags='" + featureFlags + "'}";
        }
    }

    static class KafkaMessage {
        private String userId;
        private String orderId;
        private String eventType;
        private long eventTimestamp;
        private String metadata;

        public KafkaMessage() {} // Required for Ignite Mapper

        public KafkaMessage(String userId, String orderId, String eventType, long eventTimestamp, String metadata) {
            this.userId = userId;
            this.orderId = orderId;
            this.eventType = eventType;
            this.eventTimestamp = eventTimestamp;
            this.metadata = metadata;
        }

        public String getUserId() { return userId; }
        public String getOrderId() { return orderId; }
        public String getEventType() { return eventType; }
        public long getEventTimestamp() { return eventTimestamp; }
        public String getMetadata() { return metadata; }

        @Override
        public String toString() {
            return "KafkaMessage{userId='" + userId + "', orderId='" + orderId +
                   "', eventType='" + eventType + "', eventTimestamp=" + eventTimestamp + "}";
        }
    }
}
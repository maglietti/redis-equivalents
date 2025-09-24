import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Tuple;

/**
 * Demonstrates data colocation using composite keys in Apache Ignite 3.
 *
 * Redis Cluster uses hash slots for data distribution but provides no
 * control over colocation. Ignite automatically colocates data when
 * composite primary keys share common fields, reducing network hops.
 *
 * This pattern eliminates distributed joins and enables efficient
 * batch operations on related data like user profiles and their orders.
 */
public class IgniteColocationExample {
    public static void main(String[] args) throws Exception {
        try (IgniteClient client = IgniteClient.builder()
                .addresses("127.0.0.1:10800")
                .build()) {

            demonstrateUserOrderColocation(client);
            demonstrateProductReviewColocation(client);
            demonstrateShardedCounters(client);
        }
    }

    /**
     * Colocates user profiles with their orders using user_id as affinity key.
     * Pattern: All user data stored on same node for efficient queries.
     */
    private static void demonstrateUserOrderColocation(IgniteClient client) throws Exception {
        System.out.println("=== User-Order Colocation Example ===");

        // User profiles table with user_id as both primary and affinity key
        client.sql().execute(null,
                "CREATE TABLE IF NOT EXISTS users (" +
                        "user_id VARCHAR PRIMARY KEY," +
                        "user_name VARCHAR," +
                        "email VARCHAR," +
                        "total_orders INTEGER)"
        );

        // Orders table colocated by user_id
        client.sql().execute(null,
                "CREATE TABLE IF NOT EXISTS orders (" +
                        "order_id VARCHAR," +
                        "user_id VARCHAR," +
                        "product_name VARCHAR," +
                        "order_amount DOUBLE," +
                        "order_date VARCHAR," +
                        "PRIMARY KEY (order_id, user_id))"
        );

        KeyValueView<Tuple, Tuple> userView = client.tables()
                .table("users")
                .keyValueView();

        KeyValueView<Tuple, Tuple> orderView = client.tables()
                .table("orders")
                .keyValueView();

        // Store user profiles
        storeUser(userView, "user_001", "Alice Johnson", "alice@example.com", 0);
        storeUser(userView, "user_002", "Bob Smith", "bob@example.com", 0);

        // Store orders colocated with users
        storeOrder(orderView, "order_101", "user_001", "Gaming Laptop", 1299.99, "2024-01-15");
        storeOrder(orderView, "order_102", "user_001", "Wireless Mouse", 29.99, "2024-01-16");
        storeOrder(orderView, "order_103", "user_002", "Smartphone", 799.99, "2024-01-17");

        // Efficient retrieval of colocated data
        displayUserWithOrders(userView, orderView, "user_001");
        displayUserWithOrders(userView, orderView, "user_002");

        // Update order count for user (both on same node)
        updateUserOrderCount(userView, "user_001", 2);
        updateUserOrderCount(userView, "user_002", 1);

        System.out.println();
    }

    /**
     * Colocates products with their reviews using product_id as affinity key.
     * Pattern: Product metadata and reviews stored together for fast lookups.
     */
    private static void demonstrateProductReviewColocation(IgniteClient client) throws Exception {
        System.out.println("=== Product-Review Colocation Example ===");

        // Products table
        client.sql().execute(null,
                "CREATE TABLE IF NOT EXISTS products (" +
                        "product_id VARCHAR PRIMARY KEY," +
                        "product_name VARCHAR," +
                        "price DOUBLE," +
                        "rating_avg DOUBLE," +
                        "review_count INTEGER)"
        );

        // Reviews table colocated by product_id
        client.sql().execute(null,
                "CREATE TABLE IF NOT EXISTS reviews (" +
                        "review_id VARCHAR," +
                        "product_id VARCHAR," +
                        "user_id VARCHAR," +
                        "rating INTEGER," +
                        "review_comment VARCHAR," +
                        "PRIMARY KEY (review_id, product_id))"
        );

        KeyValueView<Tuple, Tuple> productView = client.tables()
                .table("products")
                .keyValueView();

        KeyValueView<Tuple, Tuple> reviewView = client.tables()
                .table("reviews")
                .keyValueView();

        // Store products
        storeProduct(productView, "prod_001", "Gaming Laptop", 1299.99, 0.0, 0);
        storeProduct(productView, "prod_002", "Wireless Mouse", 29.99, 0.0, 0);

        // Store reviews colocated with products
        storeReview(reviewView, "rev_001", "prod_001", "user_001", 5, "Solid gaming laptop with good performance");
        storeReview(reviewView, "rev_002", "prod_001", "user_002", 4, "Good performance, bit expensive");
        storeReview(reviewView, "rev_003", "prod_002", "user_001", 5, "Responsive mouse with accurate tracking");

        // Efficient retrieval of colocated product and review data
        displayProductWithReviews(productView, reviewView, "prod_001");
        displayProductWithReviews(productView, reviewView, "prod_002");

        // Update product ratings (product and reviews on same node)
        updateProductRating(productView, "prod_001", 4.5, 2);
        updateProductRating(productView, "prod_002", 5.0, 1);

        System.out.println();
    }

    /**
     * Demonstrates sharded counters using affinity keys for load distribution.
     * Pattern: Counter shards distributed across nodes while maintaining locality.
     */
    private static void demonstrateShardedCounters(IgniteClient client) throws Exception {
        System.out.println("=== Sharded Counters with Affinity Keys ===");

        // Sharded counters table with shard_key as affinity key
        client.sql().execute(null,
                "CREATE TABLE IF NOT EXISTS sharded_counters (" +
                        "counter_name VARCHAR," +
                        "shard_key VARCHAR," +
                        "count_value BIGINT," +
                        "PRIMARY KEY (counter_name, shard_key))"
        );

        KeyValueView<Tuple, Tuple> counterView = client.tables()
                .table("sharded_counters")
                .keyValueView();

        // Initialize sharded counters for page views
        String counterName = "page_views";
        for (int shard = 0; shard < 4; shard++) {
            String shardKey = "shard_" + shard;
            storeCounter(counterView, counterName, shardKey, 0);
        }

        // Increment counters on different shards (simulating load distribution)
        incrementShardedCounter(counterView, counterName, "shard_0", 100);
        incrementShardedCounter(counterView, counterName, "shard_1", 150);
        incrementShardedCounter(counterView, counterName, "shard_2", 200);
        incrementShardedCounter(counterView, counterName, "shard_3", 175);

        // Read total count across all shards
        long totalPageViews = getTotalCounterValue(counterView, counterName);
        System.out.println("Total page views across all shards: " + totalPageViews);

        // Display individual shard values
        for (int shard = 0; shard < 4; shard++) {
            String shardKey = "shard_" + shard;
            long shardValue = getCounterValue(counterView, counterName, shardKey);
            System.out.println("Shard " + shard + " page views: " + shardValue);
        }

        System.out.println();
    }

    // Helper methods for User-Order operations
    private static void storeUser(KeyValueView<Tuple, Tuple> view, String userId, String name, String email, int totalOrders) {
        Tuple key = Tuple.create().set("user_id", userId);
        Tuple value = Tuple.create()
                .set("user_name", name)
                .set("email", email)
                .set("total_orders", totalOrders);
        view.put(null, key, value);
    }

    private static void storeOrder(KeyValueView<Tuple, Tuple> view, String orderId, String userId, String productName, double amount, String orderDate) {
        Tuple key = Tuple.create()
                .set("order_id", orderId)
                .set("user_id", userId);
        Tuple value = Tuple.create()
                .set("product_name", productName)
                .set("order_amount", amount)
                .set("order_date", orderDate);
        view.put(null, key, value);
    }

    private static void displayUserWithOrders(KeyValueView<Tuple, Tuple> userView, KeyValueView<Tuple, Tuple> orderView, String userId) {
        Tuple userKey = Tuple.create().set("user_id", userId);
        Tuple user = userView.get(null, userKey);

        if (user != null) {
            System.out.println("User: " + user.stringValue("user_name") + " (" + user.stringValue("email") + ")");
            System.out.println("Total orders: " + user.intValue("total_orders"));
        }
    }

    private static void updateUserOrderCount(KeyValueView<Tuple, Tuple> view, String userId, int orderCount) {
        Tuple key = Tuple.create().set("user_id", userId);
        Tuple currentUser = view.get(null, key);
        if (currentUser != null) {
            Tuple updatedUser = Tuple.create()
                    .set("user_name", currentUser.stringValue("user_name"))
                    .set("email", currentUser.stringValue("email"))
                    .set("total_orders", orderCount);
            view.put(null, key, updatedUser);
        }
    }

    // Helper methods for Product-Review operations
    private static void storeProduct(KeyValueView<Tuple, Tuple> view, String productId, String name, double price, double ratingAvg, int reviewCount) {
        Tuple key = Tuple.create().set("product_id", productId);
        Tuple value = Tuple.create()
                .set("product_name", name)
                .set("price", price)
                .set("rating_avg", ratingAvg)
                .set("review_count", reviewCount);
        view.put(null, key, value);
    }

    private static void storeReview(KeyValueView<Tuple, Tuple> view, String reviewId, String productId, String userId, int rating, String comment) {
        Tuple key = Tuple.create()
                .set("review_id", reviewId)
                .set("product_id", productId);
        Tuple value = Tuple.create()
                .set("user_id", userId)
                .set("rating", rating)
                .set("review_comment", comment);
        view.put(null, key, value);
    }

    private static void displayProductWithReviews(KeyValueView<Tuple, Tuple> productView, KeyValueView<Tuple, Tuple> reviewView, String productId) {
        Tuple productKey = Tuple.create().set("product_id", productId);
        Tuple product = productView.get(null, productKey);

        if (product != null) {
            System.out.println("Product: " + product.stringValue("product_name") + " ($" + product.doubleValue("price") + ")");
            System.out.println("Rating: " + product.doubleValue("rating_avg") + " (" + product.intValue("review_count") + " reviews)");
        }
    }

    private static void updateProductRating(KeyValueView<Tuple, Tuple> view, String productId, double newRating, int reviewCount) {
        Tuple key = Tuple.create().set("product_id", productId);
        Tuple currentProduct = view.get(null, key);
        if (currentProduct != null) {
            Tuple updatedProduct = Tuple.create()
                    .set("product_name", currentProduct.stringValue("product_name"))
                    .set("price", currentProduct.doubleValue("price"))
                    .set("rating_avg", newRating)
                    .set("review_count", reviewCount);
            view.put(null, key, updatedProduct);
        }
    }

    // Helper methods for Sharded Counter operations
    private static void storeCounter(KeyValueView<Tuple, Tuple> view, String counterName, String shardKey, long value) {
        Tuple key = Tuple.create()
                .set("counter_name", counterName)
                .set("shard_key", shardKey);
        Tuple valueData = Tuple.create().set("count_value", value);
        view.put(null, key, valueData);
    }

    private static void incrementShardedCounter(KeyValueView<Tuple, Tuple> view, String counterName, String shardKey, long increment) {
        Tuple key = Tuple.create()
                .set("counter_name", counterName)
                .set("shard_key", shardKey);
        Tuple currentValue = view.get(null, key);
        if (currentValue != null) {
            long newValue = currentValue.longValue("count_value") + increment;
            Tuple updatedValue = Tuple.create().set("count_value", newValue);
            view.put(null, key, updatedValue);
        }
    }

    private static long getCounterValue(KeyValueView<Tuple, Tuple> view, String counterName, String shardKey) {
        Tuple key = Tuple.create()
                .set("counter_name", counterName)
                .set("shard_key", shardKey);
        Tuple value = view.get(null, key);
        return value != null ? value.longValue("count_value") : 0;
    }

    private static long getTotalCounterValue(KeyValueView<Tuple, Tuple> view, String counterName) {
        long total = 0;
        for (int shard = 0; shard < 4; shard++) {
            String shardKey = "shard_" + shard;
            total += getCounterValue(view, counterName, shardKey);
        }
        return total;
    }
}
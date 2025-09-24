import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Tuple;

/**
 * Demonstrates primitive type key-value operations using Apache Ignite 3.
 *
 * Redis stores all data as strings requiring client-side conversion,
 * while Ignite provides native type safety for integers, longs, doubles,
 * and booleans. This eliminates parsing overhead and reduces errors.
 *
 * Common Redis caching patterns like counters, flags, and metrics
 * benefit from Ignite's native primitive support.
 */
public class IgnitePrimitiveTypesExample {
    public static void main(String[] args) throws Exception {
        try (IgniteClient client = IgniteClient.builder()
                .addresses("127.0.0.1:10800")
                .build()) {

            demonstrateIntegerKeys(client);
            demonstrateCounters(client);
            demonstrateFlags(client);
            demonstrateMetrics(client);
        }
    }

    /**
     * Shows integer keys with string values for user session data.
     * Pattern: session_id -> user_data
     */
    private static void demonstrateIntegerKeys(IgniteClient client) throws Exception {
        System.out.println("=== Integer Keys with String Values ===");

        client.sql().execute(null,
                "CREATE TABLE IF NOT EXISTS primitive_sessions (" +
                        "session_id INTEGER PRIMARY KEY," +
                        "user_data VARCHAR)"
        );

        KeyValueView<Tuple, Tuple> sessionView = client.tables()
                .table("primitive_sessions")
                .keyValueView();

        // Store session data using integer session IDs
        Tuple sessionKey1 = Tuple.create().set("session_id", 12345);
        Tuple sessionData1 = Tuple.create().set("user_data", "user:alice|role:admin");
        sessionView.put(null, sessionKey1, sessionData1);

        Tuple sessionKey2 = Tuple.create().set("session_id", 67890);
        Tuple sessionData2 = Tuple.create().set("user_data", "user:bob|role:user");
        sessionView.put(null, sessionKey2, sessionData2);

        // Retrieve session data
        Tuple retrievedData = sessionView.get(null, sessionKey1);
        String userData = retrievedData.stringValue("user_data");
        System.out.println("Session 12345: " + userData);

        // Check session exists
        boolean sessionExists = sessionView.contains(null, sessionKey2);
        System.out.println("Session 67890 exists: " + sessionExists);

        System.out.println();
    }

    /**
     * Shows string keys with integer values for counters.
     * Pattern: counter_name -> count_value
     */
    private static void demonstrateCounters(IgniteClient client) throws Exception {
        System.out.println("=== String Keys with Integer Values (Counters) ===");

        client.sql().execute(null,
                "CREATE TABLE IF NOT EXISTS counters (" +
                        "counter_name VARCHAR PRIMARY KEY," +
                        "count_value INTEGER)"
        );

        KeyValueView<Tuple, Tuple> counterView = client.tables()
                .table("counters")
                .keyValueView();

        // Initialize counters
        Tuple pageViewsKey = Tuple.create().set("counter_name", "page_views");
        Tuple pageViewsValue = Tuple.create().set("count_value", 0);
        counterView.put(null, pageViewsKey, pageViewsValue);

        Tuple loginsKey = Tuple.create().set("counter_name", "user_logins");
        Tuple loginsValue = Tuple.create().set("count_value", 0);
        counterView.put(null, loginsKey, loginsValue);

        // Increment counters (Redis INCR equivalent)
        incrementCounter(counterView, "page_views", 1);
        incrementCounter(counterView, "page_views", 5);
        incrementCounter(counterView, "user_logins", 1);

        // Read counter values
        int pageViews = getCounterValue(counterView, "page_views");
        int logins = getCounterValue(counterView, "user_logins");
        System.out.println("Page views: " + pageViews);
        System.out.println("User logins: " + logins);

        System.out.println();
    }

    /**
     * Shows string keys with boolean values for feature flags.
     * Pattern: feature_name -> enabled_status
     */
    private static void demonstrateFlags(IgniteClient client) throws Exception {
        System.out.println("=== String Keys with Boolean Values (Feature Flags) ===");

        client.sql().execute(null,
                "CREATE TABLE IF NOT EXISTS feature_flags (" +
                        "feature_name VARCHAR PRIMARY KEY," +
                        "is_enabled BOOLEAN)"
        );

        KeyValueView<Tuple, Tuple> flagView = client.tables()
                .table("feature_flags")
                .keyValueView();

        // Set feature flags
        Tuple darkModeKey = Tuple.create().set("feature_name", "dark_mode");
        Tuple darkModeValue = Tuple.create().set("is_enabled", true);
        flagView.put(null, darkModeKey, darkModeValue);

        Tuple betaFeaturesKey = Tuple.create().set("feature_name", "beta_features");
        Tuple betaFeaturesValue = Tuple.create().set("is_enabled", false);
        flagView.put(null, betaFeaturesKey, betaFeaturesValue);

        // Check feature flags
        boolean darkModeEnabled = getFlagValue(flagView, "dark_mode");
        boolean betaEnabled = getFlagValue(flagView, "beta_features");
        System.out.println("Dark mode enabled: " + darkModeEnabled);
        System.out.println("Beta features enabled: " + betaEnabled);

        // Toggle feature flag
        toggleFlag(flagView, "beta_features");
        boolean betaEnabledAfterToggle = getFlagValue(flagView, "beta_features");
        System.out.println("Beta features after toggle: " + betaEnabledAfterToggle);

        System.out.println();
    }

    /**
     * Shows string keys with double values for metrics.
     * Pattern: metric_name -> metric_value
     */
    private static void demonstrateMetrics(IgniteClient client) throws Exception {
        System.out.println("=== String Keys with Double Values (Metrics) ===");

        client.sql().execute(null,
                "CREATE TABLE IF NOT EXISTS metrics (" +
                        "metric_name VARCHAR PRIMARY KEY," +
                        "metric_value DOUBLE)"
        );

        KeyValueView<Tuple, Tuple> metricsView = client.tables()
                .table("metrics")
                .keyValueView();

        // Store performance metrics
        Tuple responseTimeKey = Tuple.create().set("metric_name", "avg_response_time");
        Tuple responseTimeValue = Tuple.create().set("metric_value", 245.7);
        metricsView.put(null, responseTimeKey, responseTimeValue);

        Tuple cpuUsageKey = Tuple.create().set("metric_name", "cpu_usage_percent");
        Tuple cpuUsageValue = Tuple.create().set("metric_value", 73.2);
        metricsView.put(null, cpuUsageKey, cpuUsageValue);

        // Read metrics
        double responseTime = getMetricValue(metricsView, "avg_response_time");
        double cpuUsage = getMetricValue(metricsView, "cpu_usage_percent");
        System.out.println("Average response time: " + responseTime + "ms");
        System.out.println("CPU usage: " + cpuUsage + "%");

        // Update metric
        updateMetric(metricsView, "avg_response_time", 198.3);
        double updatedResponseTime = getMetricValue(metricsView, "avg_response_time");
        System.out.println("Updated response time: " + updatedResponseTime + "ms");

        System.out.println();
    }

    private static void incrementCounter(KeyValueView<Tuple, Tuple> view, String counterName, int increment) {
        Tuple key = Tuple.create().set("counter_name", counterName);
        Tuple currentValue = view.get(null, key);
        int newValue = currentValue.intValue("count_value") + increment;
        Tuple updatedValue = Tuple.create().set("count_value", newValue);
        view.put(null, key, updatedValue);
    }

    private static int getCounterValue(KeyValueView<Tuple, Tuple> view, String counterName) {
        Tuple key = Tuple.create().set("counter_name", counterName);
        Tuple value = view.get(null, key);
        return value.intValue("count_value");
    }

    private static boolean getFlagValue(KeyValueView<Tuple, Tuple> view, String featureName) {
        Tuple key = Tuple.create().set("feature_name", featureName);
        Tuple value = view.get(null, key);
        return value.booleanValue("is_enabled");
    }

    private static void toggleFlag(KeyValueView<Tuple, Tuple> view, String featureName) {
        Tuple key = Tuple.create().set("feature_name", featureName);
        Tuple currentValue = view.get(null, key);
        boolean newValue = !currentValue.booleanValue("is_enabled");
        Tuple updatedValue = Tuple.create().set("is_enabled", newValue);
        view.put(null, key, updatedValue);
    }

    private static double getMetricValue(KeyValueView<Tuple, Tuple> view, String metricName) {
        Tuple key = Tuple.create().set("metric_name", metricName);
        Tuple value = view.get(null, key);
        return value.doubleValue("metric_value");
    }

    private static void updateMetric(KeyValueView<Tuple, Tuple> view, String metricName, double newValue) {
        Tuple key = Tuple.create().set("metric_name", metricName);
        Tuple value = Tuple.create().set("metric_value", newValue);
        view.put(null, key, value);
    }
}
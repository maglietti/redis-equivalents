import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Tuple;

/**
 * Demonstrates Redis sorted set operations using Apache Ignite 3 KeyValueView API.
 *
 * Maps Redis sorted set operations to Ignite composite key operations:
 * - Redis sorted set key = Ignite composite key (zset_name, member)
 * - Redis member-score pairs = Ignite key-value with score as value
 *
 * Uses composite primary key (zset_name, member) to ensure unique members
 * while storing scores as values for Redis sorted set semantics.
 */
public class IgniteSortedSetExample {
    public static void main(String[] args) throws Exception {
        try (IgniteClient client = IgniteClient.builder()
                .addresses("127.0.0.1:10800")
                .build()) {

            client.sql().execute(null,
                "CREATE TABLE IF NOT EXISTS zset_table (" +
                "zset_name VARCHAR," +
                "member VARCHAR," +
                "score DOUBLE," +
                "PRIMARY KEY (zset_name, member))"
            );

            KeyValueView<Tuple, Tuple> kvView = client.tables()
                    .table("zset_table")
                    .keyValueView();

            String zsetName = "leaderboard";

            // Redis ZADD equivalent: ZADD leaderboard 100 "player1"
            zadd(kvView, zsetName, "player1", 100.0);
            System.out.println("ZADD " + zsetName + " 100 'player1'");

            // Redis ZADD equivalent: ZADD leaderboard 250 "player2"
            zadd(kvView, zsetName, "player2", 250.0);
            System.out.println("ZADD " + zsetName + " 250 'player2'");

            // Redis ZADD equivalent: ZADD leaderboard 180 "player3"
            zadd(kvView, zsetName, "player3", 180.0);
            System.out.println("ZADD " + zsetName + " 180 'player3'");

            // Redis ZSCORE equivalent: ZSCORE leaderboard "player2"
            Double player2Score = zscore(kvView, zsetName, "player2");
            System.out.println("ZSCORE " + zsetName + " 'player2' -> " + player2Score);

            // Redis ZINCRBY equivalent: ZINCRBY leaderboard 50 "player1"
            double newScore = zincrby(kvView, zsetName, "player1", 50.0);
            System.out.println("ZINCRBY " + zsetName + " 50 'player1' -> " + newScore);

            // Verify the increment worked
            Double updatedScore = zscore(kvView, zsetName, "player1");
            System.out.println("ZSCORE " + zsetName + " 'player1' -> " + updatedScore);

            // Redis ZREM equivalent: ZREM leaderboard "player3"
            boolean removed = zrem(kvView, zsetName, "player3");
            System.out.println("ZREM " + zsetName + " 'player3' -> " + removed);

            // Redis ZCARD equivalent: get cardinality (count of members)
            long cardinality = zcard(kvView, zsetName);
            System.out.println("ZCARD " + zsetName + " -> " + cardinality);

            // Redis ZRANK equivalent: get rank of member (0-based)
            // Note: This requires scanning to find rank by score
            Integer player1Rank = zrank(kvView, zsetName, "player1");
            System.out.println("ZRANK " + zsetName + " 'player1' -> " + player1Rank);

            Integer player2Rank = zrank(kvView, zsetName, "player2");
            System.out.println("ZRANK " + zsetName + " 'player2' -> " + player2Rank);

            // Display current sorted set state
            System.out.println("Current leaderboard:");
            printSortedSet(kvView, zsetName);
        }
    }

    private static void zadd(KeyValueView<Tuple, Tuple> kvView, String zsetName, String member, double score) {
        Tuple key = Tuple.create()
                .set("zset_name", zsetName)
                .set("member", member);
        Tuple value = Tuple.create().set("score", score);
        kvView.put(null, key, value);
    }

    private static Double zscore(KeyValueView<Tuple, Tuple> kvView, String zsetName, String member) {
        Tuple key = Tuple.create()
                .set("zset_name", zsetName)
                .set("member", member);
        Tuple value = kvView.get(null, key);
        return value != null ? value.doubleValue("score") : null;
    }

    private static double zincrby(KeyValueView<Tuple, Tuple> kvView, String zsetName, String member, double increment) {
        Double currentScore = zscore(kvView, zsetName, member);
        double newScore = (currentScore != null ? currentScore : 0.0) + increment;
        zadd(kvView, zsetName, member, newScore);
        return newScore;
    }

    private static boolean zrem(KeyValueView<Tuple, Tuple> kvView, String zsetName, String member) {
        Tuple key = Tuple.create()
                .set("zset_name", zsetName)
                .set("member", member);
        return kvView.remove(null, key);
    }

    private static long zcard(KeyValueView<Tuple, Tuple> kvView, String zsetName) {
        // Note: This implementation scans for count
        // In production, you might maintain a separate counter
        long count = 0;
        // This would require scanning through possible members or maintaining metadata
        // For demo purposes, we'll scan a reasonable range
        String[] testMembers = {"player1", "player2", "player3", "player4", "player5"};

        for (String member : testMembers) {
            Tuple key = Tuple.create()
                    .set("zset_name", zsetName)
                    .set("member", member);
            if (kvView.get(null, key) != null) {
                count++;
            }
        }
        return count;
    }

    private static Integer zrank(KeyValueView<Tuple, Tuple> kvView, String zsetName, String targetMember) {
        // Note: This implementation uses scanning
        // In production, you would maintain a more efficient rank structure
        Double targetScore = zscore(kvView, zsetName, targetMember);
        if (targetScore == null) return null;

        int rank = 0;
        String[] testMembers = {"player1", "player2", "player3", "player4", "player5"};

        for (String member : testMembers) {
            Double memberScore = zscore(kvView, zsetName, member);
            if (memberScore != null && memberScore < targetScore) {
                rank++;
            }
        }
        return rank;
    }

    private static void printSortedSet(KeyValueView<Tuple, Tuple> kvView, String zsetName) {
        String[] testMembers = {"player1", "player2", "player3", "player4", "player5"};

        java.util.List<java.util.Map.Entry<String, Double>> entries = new java.util.ArrayList<>();

        for (String member : testMembers) {
            Double score = zscore(kvView, zsetName, member);
            if (score != null) {
                entries.add(new java.util.AbstractMap.SimpleEntry<>(member, score));
            }
        }

        entries.sort(java.util.Map.Entry.comparingByValue());

        for (int i = 0; i < entries.size(); i++) {
            java.util.Map.Entry<String, Double> entry = entries.get(i);
            System.out.println("[" + i + "] " + entry.getKey() + " (" + entry.getValue() + ")");
        }
    }
}
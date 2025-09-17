import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Tuple;

/**
 * Demonstrates Redis queue operations using Apache Ignite 3 KeyValueView API.
 *
 * Maps Redis queue operations to Ignite composite key operations:
 * - Redis queue uses LPUSH/RPOP or RPUSH/LPOP for FIFO behavior
 * - Ignite composite key (queue_name, sequence_number) maintains order
 *
 * Uses composite primary key (queue_name, seq) for efficient FIFO queue operations
 * while leveraging Ignite's KeyValueView API pattern.
 */
public class IgniteQueueExample {
    public static void main(String[] args) throws Exception {
        try (IgniteClient client = IgniteClient.builder()
                .addresses("127.0.0.1:10800")
                .build()) {

            client.sql().execute(null,
                "CREATE TABLE IF NOT EXISTS queue_table (" +
                "queue_name VARCHAR," +
                "seq BIGINT," +
                "val VARCHAR," +
                "PRIMARY KEY (queue_name, seq))"
            );

            client.sql().execute(null,
                "CREATE TABLE IF NOT EXISTS queue_meta (" +
                "queue_name VARCHAR PRIMARY KEY," +
                "head_seq BIGINT," +
                "tail_seq BIGINT)"
            );

            KeyValueView<Tuple, Tuple> kvView = client.tables()
                    .table("queue_table")
                    .keyValueView();

            KeyValueView<Tuple, Tuple> metaView = client.tables()
                    .table("queue_meta")
                    .keyValueView();

            String queueName = "jobs";

            initQueue(metaView, queueName);

            // Redis LPUSH equivalent (enqueue): Add items to tail
            enqueue(kvView, metaView, queueName, "job1");
            System.out.println("ENQUEUE " + queueName + " 'job1'");

            enqueue(kvView, metaView, queueName, "job2");
            System.out.println("ENQUEUE " + queueName + " 'job2'");

            enqueue(kvView, metaView, queueName, "job3");
            System.out.println("ENQUEUE " + queueName + " 'job3'");

            // Redis RPOP equivalent (dequeue): Remove items from head (FIFO)
            String firstJob = dequeue(kvView, metaView, queueName);
            System.out.println("DEQUEUE " + queueName + " -> " + firstJob);

            String secondJob = dequeue(kvView, metaView, queueName);
            System.out.println("DEQUEUE " + queueName + " -> " + secondJob);

            // Check queue size
            long size = getQueueSize(metaView, queueName);
            System.out.println("Queue size: " + size);

            // Peek at next item without removing
            String nextJob = peek(kvView, metaView, queueName);
            System.out.println("PEEK " + queueName + " -> " + nextJob);

            // Queue size should remain the same after peek
            size = getQueueSize(metaView, queueName);
            System.out.println("Queue size after peek: " + size);
        }
    }

    private static void initQueue(KeyValueView<Tuple, Tuple> metaView, String queueName) {
        Tuple metaKey = Tuple.create().set("queue_name", queueName);
        Tuple existingMeta = metaView.get(null, metaKey);

        if (existingMeta == null) {
            Tuple metaValue = Tuple.create()
                    .set("head_seq", 0L)
                    .set("tail_seq", 0L);
            metaView.put(null, metaKey, metaValue);
        }
    }

    private static void enqueue(KeyValueView<Tuple, Tuple> kvView, KeyValueView<Tuple, Tuple> metaView,
                               String queueName, String value) {
        Tuple metaKey = Tuple.create().set("queue_name", queueName);
        Tuple metaValue = metaView.get(null, metaKey);

        long tailSeq = metaValue.longValue("tail_seq");

        Tuple itemKey = Tuple.create()
                .set("queue_name", queueName)
                .set("seq", tailSeq);
        Tuple itemValue = Tuple.create().set("val", value);
        kvView.put(null, itemKey, itemValue);

        Tuple updatedMeta = Tuple.create()
                .set("head_seq", metaValue.longValue("head_seq"))
                .set("tail_seq", tailSeq + 1);
        metaView.put(null, metaKey, updatedMeta);
    }

    private static String dequeue(KeyValueView<Tuple, Tuple> kvView, KeyValueView<Tuple, Tuple> metaView,
                                 String queueName) {
        Tuple metaKey = Tuple.create().set("queue_name", queueName);
        Tuple metaValue = metaView.get(null, metaKey);

        long headSeq = metaValue.longValue("head_seq");
        long tailSeq = metaValue.longValue("tail_seq");

        if (headSeq >= tailSeq) {
            return null; // Queue is empty
        }

        Tuple itemKey = Tuple.create()
                .set("queue_name", queueName)
                .set("seq", headSeq);
        Tuple itemValue = kvView.get(null, itemKey);
        kvView.remove(null, itemKey);

        Tuple updatedMeta = Tuple.create()
                .set("head_seq", headSeq + 1)
                .set("tail_seq", tailSeq);
        metaView.put(null, metaKey, updatedMeta);

        return itemValue != null ? itemValue.stringValue("val") : null;
    }

    private static String peek(KeyValueView<Tuple, Tuple> kvView, KeyValueView<Tuple, Tuple> metaView,
                              String queueName) {
        Tuple metaKey = Tuple.create().set("queue_name", queueName);
        Tuple metaValue = metaView.get(null, metaKey);

        long headSeq = metaValue.longValue("head_seq");
        long tailSeq = metaValue.longValue("tail_seq");

        if (headSeq >= tailSeq) {
            return null; // Queue is empty
        }

        Tuple itemKey = Tuple.create()
                .set("queue_name", queueName)
                .set("seq", headSeq);
        Tuple itemValue = kvView.get(null, itemKey);

        return itemValue != null ? itemValue.stringValue("val") : null;
    }

    private static long getQueueSize(KeyValueView<Tuple, Tuple> metaView, String queueName) {
        Tuple metaKey = Tuple.create().set("queue_name", queueName);
        Tuple metaValue = metaView.get(null, metaKey);

        long headSeq = metaValue.longValue("head_seq");
        long tailSeq = metaValue.longValue("tail_seq");

        return tailSeq - headSeq;
    }
}
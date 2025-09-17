import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Tuple;

/**
 * Demonstrates Redis list operations using Apache Ignite 3 KeyValueView API.
 *
 * Maps Redis list operations to Ignite composite key operations:
 * - Redis list key = Ignite composite key (list_name, index)
 * - Redis list elements = Ignite values with auto-incrementing composite keys
 *
 * Uses composite primary key (list_name, index) to maintain Redis list ordering
 * while leveraging Ignite's KeyValueView API pattern.
 */
public class IgniteListExample {
    public static void main(String[] args) throws Exception {
        try (IgniteClient client = IgniteClient.builder()
                .addresses("127.0.0.1:10800")
                .build()) {

            client.sql().execute(null,
                "CREATE TABLE IF NOT EXISTS list_table (" +
                "list_name VARCHAR," +
                "list_index INTEGER," +
                "val VARCHAR," +
                "PRIMARY KEY (list_name, list_index))"
            );

            KeyValueView<Tuple, Tuple> kvView = client.tables()
                    .table("list_table")
                    .keyValueView();

            String listName = "tasks";

            // Redis LPUSH equivalent: LPUSH tasks "task1"
            // Insert at index 0, shifting existing elements right
            pushLeft(kvView, listName, "task1");
            System.out.println("LPUSH " + listName + " 'task1'");

            // Redis LPUSH equivalent: LPUSH tasks "task2"
            pushLeft(kvView, listName, "task2");
            System.out.println("LPUSH " + listName + " 'task2'");

            // Redis RPUSH equivalent: RPUSH tasks "task3"
            pushRight(kvView, listName, "task3");
            System.out.println("RPUSH " + listName + " 'task3'");

            // Redis LINDEX equivalent: LINDEX tasks 0
            String firstElement = getByIndex(kvView, listName, 0);
            System.out.println("LINDEX " + listName + " 0 -> " + firstElement);

            // Redis LINDEX equivalent: LINDEX tasks 1
            String secondElement = getByIndex(kvView, listName, 1);
            System.out.println("LINDEX " + listName + " 1 -> " + secondElement);

            // Redis LINDEX equivalent: LINDEX tasks -1 (last element)
            String lastElement = getByIndex(kvView, listName, -1);
            System.out.println("LINDEX " + listName + " -1 -> " + lastElement);

            // Redis LPOP equivalent: LPOP tasks
            String popped = popLeft(kvView, listName);
            System.out.println("LPOP " + listName + " -> " + popped);

            // Verify list state after pop
            System.out.println("List after LPOP:");
            printList(kvView, listName);
        }
    }

    private static void pushLeft(KeyValueView<Tuple, Tuple> kvView, String listName, String value) {
        shiftElementsRight(kvView, listName);

        Tuple key = Tuple.create()
                .set("list_name", listName)
                .set("list_index", 0);
        Tuple val = Tuple.create().set("val", value);
        kvView.put(null, key, val);
    }

    private static void pushRight(KeyValueView<Tuple, Tuple> kvView, String listName, String value) {
        int nextIndex = getListSize(kvView, listName);

        Tuple key = Tuple.create()
                .set("list_name", listName)
                .set("list_index", nextIndex);
        Tuple val = Tuple.create().set("val", value);
        kvView.put(null, key, val);
    }

    private static String getByIndex(KeyValueView<Tuple, Tuple> kvView, String listName, int index) {
        if (index < 0) {
            int size = getListSize(kvView, listName);
            index = size + index;
        }

        Tuple key = Tuple.create()
                .set("list_name", listName)
                .set("list_index", index);
        Tuple value = kvView.get(null, key);
        return value != null ? value.stringValue("val") : null;
    }

    private static String popLeft(KeyValueView<Tuple, Tuple> kvView, String listName) {
        Tuple key = Tuple.create()
                .set("list_name", listName)
                .set("list_index", 0);
        Tuple value = kvView.get(null, key);
        if (value == null) return null;

        kvView.remove(null, key);
        shiftElementsLeft(kvView, listName);

        return value.stringValue("val");
    }

    private static void shiftElementsRight(KeyValueView<Tuple, Tuple> kvView, String listName) {
        int size = getListSize(kvView, listName);

        for (int i = size - 1; i >= 0; i--) {
            Tuple oldKey = Tuple.create()
                    .set("list_name", listName)
                    .set("list_index", i);
            Tuple value = kvView.get(null, oldKey);

            if (value != null) {
                kvView.remove(null, oldKey);

                Tuple newKey = Tuple.create()
                        .set("list_name", listName)
                        .set("list_index", i + 1);
                kvView.put(null, newKey, value);
            }
        }
    }

    private static void shiftElementsLeft(KeyValueView<Tuple, Tuple> kvView, String listName) {
        int size = getListSize(kvView, listName);

        for (int i = 1; i < size; i++) {
            Tuple oldKey = Tuple.create()
                    .set("list_name", listName)
                    .set("list_index", i);
            Tuple value = kvView.get(null, oldKey);

            if (value != null) {
                kvView.remove(null, oldKey);

                Tuple newKey = Tuple.create()
                        .set("list_name", listName)
                        .set("list_index", i - 1);
                kvView.put(null, newKey, value);
            }
        }
    }

    private static int getListSize(KeyValueView<Tuple, Tuple> kvView, String listName) {
        int size = 0;
        while (true) {
            Tuple key = Tuple.create()
                    .set("list_name", listName)
                    .set("list_index", size);
            if (kvView.get(null, key) == null) break;
            size++;
        }
        return size;
    }

    private static void printList(KeyValueView<Tuple, Tuple> kvView, String listName) {
        int size = getListSize(kvView, listName);
        for (int i = 0; i < size; i++) {
            String element = getByIndex(kvView, listName, i);
            System.out.println("[" + i + "] " + element);
        }
    }
}
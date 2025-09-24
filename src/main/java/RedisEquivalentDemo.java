/**
 * Runs all Redis data structure and key-value pattern demonstrations sequentially.
 *
 * Executes each example class to show Redis operations and advanced patterns
 * implemented using Apache Ignite 3's KeyValueView and RecordView APIs.
 *
 * Provides a single entry point to demonstrate Redis data structures
 * plus key scenarios for real-world caching and key-value applications.
 */
public class RedisEquivalentDemo {
    public static void main(String[] args) throws Exception {
        System.out.println("=== Redis Data Structure and Key-Value Patterns in Apache Ignite 3 ===\n");

        // Core Redis data structures
        System.out.println("[1/12] Key-Value Store Example");
        System.out.println("-----------------------------");
        IgniteKeyValueExample.main(args);
        System.out.println();

        System.out.println("[2/12] Hash Example");
        System.out.println("-----------------");
        IgniteHashExample.main(args);
        System.out.println();

        System.out.println("[3/12] List Example");
        System.out.println("-----------------");
        IgniteListExample.main(args);
        System.out.println();

        System.out.println("[4/12] Queue Example");
        System.out.println("------------------");
        IgniteQueueExample.main(args);
        System.out.println();

        System.out.println("[5/12] Set Example");
        System.out.println("----------------");
        IgniteSetExample.main(args);
        System.out.println();

        System.out.println("[6/12] Sorted Set Example");
        System.out.println("------------------------");
        IgniteSortedSetExample.main(args);
        System.out.println();

        // Advanced key-value patterns
        System.out.println("[7/12] Primitive Types Example");
        System.out.println("-----------------------------");
        IgnitePrimitiveTypesExample.main(args);
        System.out.println();

        System.out.println("[8/12] POJO Objects Example");
        System.out.println("--------------------------");
        IgnitePojoExample.main(args);
        System.out.println();

        System.out.println("[9/12] Data Colocation Example");
        System.out.println("-----------------------------");
        IgniteColocationExample.main(args);
        System.out.println();

        System.out.println("[10/12] Serialization Example");
        System.out.println("----------------------------");
        IgniteSerializationExample.main(args);
        System.out.println();

        System.out.println("[11/12] Large Objects Example");
        System.out.println("----------------------------");
        IgniteLargeObjectsExample.main(args);
        System.out.println();

        System.out.println("[12/12] Record View Example");
        System.out.println("--------------------------");
        IgniteRecordViewExample.main(args);
        System.out.println();

        System.out.println("=== All Redis equivalents and key-value patterns completed successfully ===");
    }
}
/**
 * Runs all Redis data structure equivalents demonstrations sequentially.
 *
 * Executes each example class to show Redis operations implemented
 * using Apache Ignite 3's KeyValueView API with composite keys.
 *
 * Provides a single entry point to demonstrate all 6 Redis data structures
 * while maintaining individual example classes for focused learning.
 */
public class RedisEquivalentDemo {
    public static void main(String[] args) throws Exception {
        System.out.println("=== Redis Data Structure Equivalents in Apache Ignite 3 ===\n");

        System.out.println("[1/6] Key-Value Store Example");
        System.out.println("----------------------------");
        IgniteKeyValueExample.main(args);
        System.out.println();

        System.out.println("[2/6] Hash Example");
        System.out.println("--------------");
        IgniteHashExample.main(args);
        System.out.println();

        System.out.println("[3/6] List Example");
        System.out.println("--------------");
        IgniteListExample.main(args);
        System.out.println();

        System.out.println("[4/6] Queue Example");
        System.out.println("---------------");
        IgniteQueueExample.main(args);
        System.out.println();

        System.out.println("[5/6] Set Example");
        System.out.println("-------------");
        IgniteSetExample.main(args);
        System.out.println();

        System.out.println("[6/6] Sorted Set Example");
        System.out.println("---------------------");
        IgniteSortedSetExample.main(args);
        System.out.println();

        System.out.println("=== All Redis equivalents completed successfully ===");
    }
}
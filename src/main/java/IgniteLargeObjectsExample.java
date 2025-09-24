import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Tuple;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Demonstrates large object storage patterns using Apache Ignite 3.
 *
 * Redis has a default value size limit of 512MB but performance degrades
 * with large values. Ignite handles large objects efficiently with
 * compression and streaming capabilities for better throughput.
 *
 * This pattern supports file storage, document management, and
 * compressed data scenarios. Demo uses smaller sizes for compatibility.
 */
public class IgniteLargeObjectsExample {
    public static void main(String[] args) throws Exception {
        try (IgniteClient client = IgniteClient.builder()
                .addresses("127.0.0.1:10800")
                .build()) {

            demonstrateDocumentStorage(client);
            demonstrateFileStorage(client);
            demonstrateCompressedData(client);
        }
    }

    /**
     * Stores large text documents with metadata.
     * Pattern: Document content with compression and metadata tracking.
     */
    private static void demonstrateDocumentStorage(IgniteClient client) throws Exception {
        System.out.println("=== Large Document Storage Example ===");

        // VARBINARY defaults to 65536 bytes (64KB) limit in Ignite 3
        client.sql().execute(null,
                "CREATE TABLE IF NOT EXISTS documents (" +
                        "doc_id VARCHAR PRIMARY KEY," +
                        "metadata VARCHAR," +
                        "content VARBINARY," +
                        "original_size BIGINT," +
                        "compressed_size BIGINT)"
        );

        KeyValueView<Tuple, Tuple> docView = client.tables()
                .table("documents")
                .keyValueView();

        // Create large text documents (sized for demo within 64KB compressed limit)
        String largeArticle = generateLargeArticle(20000); // ~20KB article
        String technicalManual = generateTechnicalManual(30000); // ~30KB manual
        String logData = generateLogData(25000); // ~25KB log data

        // Store documents with compression
        storeDocument(docView, "article:tech_trends_2024", "Tech Trends 2024|author:John Doe|category:technology", largeArticle);
        storeDocument(docView, "manual:api_reference", "API Reference Manual|version:2.1|department:engineering", technicalManual);
        storeDocument(docView, "logs:app_server_001", "Application Server Logs|date:2024-01-15|level:INFO", logData);

        // Retrieve documents and show compression ratios
        DocumentInfo article = getDocument(docView, "article:tech_trends_2024");
        DocumentInfo manual = getDocument(docView, "manual:api_reference");
        DocumentInfo logs = getDocument(docView, "logs:app_server_001");

        System.out.println("Tech Article - Original: " + article.originalSize + " bytes, Compressed: " +
                          article.compressedSize + " bytes (" + getCompressionRatio(article) + "% reduction)");
        System.out.println("API Manual - Original: " + manual.originalSize + " bytes, Compressed: " +
                          manual.compressedSize + " bytes (" + getCompressionRatio(manual) + "% reduction)");
        System.out.println("Log Data - Original: " + logs.originalSize + " bytes, Compressed: " +
                          logs.compressedSize + " bytes (" + getCompressionRatio(logs) + "% reduction)");

        // Demonstrate partial content access without full decompression
        String articlePreview = getDocumentPreview(article.content, 200);
        System.out.println("Article preview: " + articlePreview + "...");

        System.out.println();
    }

    /**
     * Stores binary file data with compression and metadata.
     * Pattern: File storage with MIME type detection and compression.
     */
    private static void demonstrateFileStorage(IgniteClient client) throws Exception {
        System.out.println("=== Binary File Storage Example ===");

        client.sql().execute(null,
                "CREATE TABLE IF NOT EXISTS file_storage (" +
                        "file_id VARCHAR PRIMARY KEY," +
                        "filename VARCHAR," +
                        "mime_type VARCHAR," +
                        "file_data VARBINARY," +
                        "original_size BIGINT," +
                        "compressed_size BIGINT," +
                        "upload_timestamp BIGINT)"
        );

        KeyValueView<Tuple, Tuple> fileView = client.tables()
                .table("file_storage")
                .keyValueView();

        // Create synthetic binary files (sized for demo within 64KB compressed limit)
        byte[] imageData = generateSyntheticImageData(30 * 1024); // 30KB image
        byte[] videoData = generateSyntheticVideoData(40 * 1024); // 40KB video
        byte[] archiveData = generateSyntheticArchiveData(35 * 1024); // 35KB archive

        // Store files with compression
        storeFile(fileView, "img_001", "profile_photo.jpg", "image/jpeg", imageData);
        storeFile(fileView, "vid_001", "tutorial_video.mp4", "video/mp4", videoData);
        storeFile(fileView, "arc_001", "backup_data.zip", "application/zip", archiveData);

        // Retrieve files and show storage efficiency
        FileInfo image = getFile(fileView, "img_001");
        FileInfo video = getFile(fileView, "vid_001");
        FileInfo archive = getFile(fileView, "arc_001");

        System.out.println("Image File - " + image.filename + " (" + image.mimeType + ")");
        System.out.println("  Original: " + formatBytes(image.originalSize) + ", Compressed: " +
                          formatBytes(image.compressedSize) + " (" + getCompressionRatio(image.originalSize, image.compressedSize) + "% reduction)");

        System.out.println("Video File - " + video.filename + " (" + video.mimeType + ")");
        System.out.println("  Original: " + formatBytes(video.originalSize) + ", Compressed: " +
                          formatBytes(video.compressedSize) + " (" + getCompressionRatio(video.originalSize, video.compressedSize) + "% reduction)");

        System.out.println("Archive File - " + archive.filename + " (" + archive.mimeType + ")");
        System.out.println("  Original: " + formatBytes(archive.originalSize) + ", Compressed: " +
                          formatBytes(archive.compressedSize) + " (" + getCompressionRatio(archive.originalSize, archive.compressedSize) + "% reduction)");

        System.out.println();
    }

    /**
     * Demonstrates handling of already compressed data.
     * Pattern: Efficient storage of pre-compressed content.
     */
    private static void demonstrateCompressedData(IgniteClient client) throws Exception {
        System.out.println("=== Pre-compressed Data Example ===");

        client.sql().execute(null,
                "CREATE TABLE IF NOT EXISTS compressed_cache (" +
                        "cache_key VARCHAR PRIMARY KEY," +
                        "data_type VARCHAR," +
                        "compressed_data VARBINARY," +
                        "data_size BIGINT," +
                        "compression_type VARCHAR)"
        );

        KeyValueView<Tuple, Tuple> compressedView = client.tables()
                .table("compressed_cache")
                .keyValueView();

        // Store pre-compressed data (simulating data from external systems)
        String xmlReport = generateXmlReport(20000); // XML report
        String jsonAnalytics = generateJsonAnalytics(25000); // Analytics JSON
        String csvDataSet = generateCsvDataSet(30000); // CSV dataset

        // Store with different compression strategies
        storeCompressedData(compressedView, "report:monthly_sales", "xml", xmlReport, "gzip");
        storeCompressedData(compressedView, "analytics:user_behavior", "json", jsonAnalytics, "gzip");
        storeCompressedData(compressedView, "dataset:transaction_logs", "csv", csvDataSet, "gzip");

        // Retrieve and process compressed data
        CompressedDataInfo xmlData = getCompressedData(compressedView, "report:monthly_sales");
        CompressedDataInfo jsonData = getCompressedData(compressedView, "analytics:user_behavior");
        CompressedDataInfo csvData = getCompressedData(compressedView, "dataset:transaction_logs");

        System.out.println("XML Report (" + xmlData.compressionType + ") - Size: " + formatBytes(xmlData.dataSize));
        System.out.println("JSON Analytics (" + jsonData.compressionType + ") - Size: " + formatBytes(jsonData.dataSize));
        System.out.println("CSV Dataset (" + csvData.compressionType + ") - Size: " + formatBytes(csvData.dataSize));

        // Demonstrate streaming decompression for large data
        processCompressedDataStream(xmlData, "Processing XML report");
        processCompressedDataStream(jsonData, "Processing JSON analytics");

        System.out.println();
    }

    // Document storage helpers
    private static void storeDocument(KeyValueView<Tuple, Tuple> view, String docId, String metadata, String content) throws IOException {
        byte[] originalBytes = content.getBytes(StandardCharsets.UTF_8);
        byte[] compressed = compressData(originalBytes);

        Tuple key = Tuple.create().set("doc_id", docId);
        Tuple value = Tuple.create()
                .set("metadata", metadata)
                .set("content", compressed)
                .set("original_size", (long) originalBytes.length)
                .set("compressed_size", (long) compressed.length);
        view.put(null, key, value);
    }

    private static DocumentInfo getDocument(KeyValueView<Tuple, Tuple> view, String docId) {
        Tuple key = Tuple.create().set("doc_id", docId);
        Tuple value = view.get(null, key);
        if (value != null) {
            return new DocumentInfo(
                value.stringValue("metadata"),
                value.value("content"),
                value.longValue("original_size"),
                value.longValue("compressed_size")
            );
        }
        return null;
    }

    private static String getDocumentPreview(byte[] compressedContent, int previewLength) throws IOException {
        byte[] decompressed = decompressData(compressedContent);
        String content = new String(decompressed, StandardCharsets.UTF_8);
        return content.length() > previewLength ? content.substring(0, previewLength) : content;
    }

    // File storage helpers
    private static void storeFile(KeyValueView<Tuple, Tuple> view, String fileId, String filename, String mimeType, byte[] fileData) throws IOException {
        byte[] compressed = compressData(fileData);

        Tuple key = Tuple.create().set("file_id", fileId);
        Tuple value = Tuple.create()
                .set("filename", filename)
                .set("mime_type", mimeType)
                .set("file_data", compressed)
                .set("original_size", (long) fileData.length)
                .set("compressed_size", (long) compressed.length)
                .set("upload_timestamp", System.currentTimeMillis());
        view.put(null, key, value);
    }

    private static FileInfo getFile(KeyValueView<Tuple, Tuple> view, String fileId) {
        Tuple key = Tuple.create().set("file_id", fileId);
        Tuple value = view.get(null, key);
        if (value != null) {
            return new FileInfo(
                value.stringValue("filename"),
                value.stringValue("mime_type"),
                value.value("file_data"),
                value.longValue("original_size"),
                value.longValue("compressed_size"),
                value.longValue("upload_timestamp")
            );
        }
        return null;
    }

    // Compressed data helpers
    private static void storeCompressedData(KeyValueView<Tuple, Tuple> view, String cacheKey, String dataType, String data, String compressionType) throws IOException {
        byte[] compressed = compressData(data.getBytes(StandardCharsets.UTF_8));

        Tuple key = Tuple.create().set("cache_key", cacheKey);
        Tuple value = Tuple.create()
                .set("data_type", dataType)
                .set("compressed_data", compressed)
                .set("data_size", (long) compressed.length)
                .set("compression_type", compressionType);
        view.put(null, key, value);
    }

    private static CompressedDataInfo getCompressedData(KeyValueView<Tuple, Tuple> view, String cacheKey) {
        Tuple key = Tuple.create().set("cache_key", cacheKey);
        Tuple value = view.get(null, key);
        if (value != null) {
            return new CompressedDataInfo(
                value.stringValue("data_type"),
                value.value("compressed_data"),
                value.longValue("data_size"),
                value.stringValue("compression_type")
            );
        }
        return null;
    }

    private static void processCompressedDataStream(CompressedDataInfo data, String description) throws IOException {
        System.out.println(description + " - streaming " + formatBytes(data.dataSize) + " of compressed data");
        // Simulate streaming processing without loading entire content into memory
        try (ByteArrayInputStream bais = new ByteArrayInputStream(data.compressedData);
             GZIPInputStream gzis = new GZIPInputStream(bais);
             BufferedReader reader = new BufferedReader(new InputStreamReader(gzis, StandardCharsets.UTF_8))) {

            String line;
            int lineCount = 0;
            while ((line = reader.readLine()) != null && lineCount < 3) {
                System.out.println("  Line " + (lineCount + 1) + ": " +
                                 (line.length() > 80 ? line.substring(0, 80) + "..." : line));
                lineCount++;
            }
        }
    }

    // Compression utilities
    private static byte[] compressData(byte[] data) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             GZIPOutputStream gzos = new GZIPOutputStream(baos)) {
            gzos.write(data);
            gzos.finish();
            return baos.toByteArray();
        }
    }

    private static byte[] decompressData(byte[] compressedData) throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(compressedData);
             GZIPInputStream gzis = new GZIPInputStream(bais);
             ByteArrayOutputStream baos = new ByteArrayOutputStream()) {

            byte[] buffer = new byte[1024];
            int len;
            while ((len = gzis.read(buffer)) != -1) {
                baos.write(buffer, 0, len);
            }
            return baos.toByteArray();
        }
    }

    // Data generators for synthetic content
    private static String generateLargeArticle(int targetSize) {
        StringBuilder sb = new StringBuilder();
        String[] paragraphs = {
            "The evolution of cloud computing has transformed how businesses approach data storage and processing.",
            "Artificial intelligence and machine learning are transforming industries across the globe.",
            "Cybersecurity remains a critical concern as digital transformation accelerates.",
            "The Internet of Things connects billions of devices creating vast data streams.",
            "Blockchain technology promises to reshape financial and supply chain systems."
        };

        while (sb.length() < targetSize) {
            for (String paragraph : paragraphs) {
                sb.append(paragraph).append(" ");
                if (sb.length() >= targetSize) break;
            }
        }
        return sb.toString();
    }

    private static String generateTechnicalManual(int targetSize) {
        StringBuilder sb = new StringBuilder();
        sb.append("# API Reference Manual\n\n");

        for (int i = 1; i <= 50 && sb.length() < targetSize; i++) {
            sb.append("## Section ").append(i).append(": API Endpoint Documentation\n\n");
            sb.append("### Endpoint: /api/v1/resource").append(i).append("\n\n");
            sb.append("**Method:** GET/POST/PUT/DELETE\n\n");
            sb.append("**Description:** This endpoint handles operations for resource type ").append(i).append(".\n\n");
            sb.append("**Parameters:**\n");
            sb.append("- id (required): Resource identifier\n");
            sb.append("- filter (optional): Query filter expression\n");
            sb.append("- limit (optional): Maximum number of results\n\n");
            sb.append("**Response Format:**\n");
            sb.append("```json\n{\n  \"status\": \"success\",\n  \"data\": [...],\n  \"pagination\": {...}\n}\n```\n\n");
        }
        return sb.toString();
    }

    private static String generateLogData(int targetSize) {
        StringBuilder sb = new StringBuilder();
        String[] logLevels = {"INFO", "WARN", "ERROR", "DEBUG"};
        String[] components = {"UserService", "OrderService", "PaymentService", "NotificationService"};

        while (sb.length() < targetSize) {
            long timestamp = System.currentTimeMillis() - (long)(Math.random() * 86400000);
            String level = logLevels[(int)(Math.random() * logLevels.length)];
            String component = components[(int)(Math.random() * components.length)];
            sb.append(timestamp).append(" [").append(level).append("] ").append(component)
              .append(": Processing request ").append((int)(Math.random() * 10000))
              .append(" - Duration: ").append((int)(Math.random() * 1000)).append("ms\n");
        }
        return sb.toString();
    }

    private static byte[] generateSyntheticImageData(int size) {
        byte[] data = new byte[size];
        Random random = new Random();
        random.nextBytes(data);
        // Simulate image header patterns
        data[0] = (byte) 0xFF; data[1] = (byte) 0xD8; data[2] = (byte) 0xFF; // JPEG header
        return data;
    }

    private static byte[] generateSyntheticVideoData(int size) {
        byte[] data = new byte[size];
        Random random = new Random();
        random.nextBytes(data);
        // Simulate video header patterns
        data[0] = 0x00; data[1] = 0x00; data[2] = 0x00; data[3] = 0x20; // MP4 header
        return data;
    }

    private static byte[] generateSyntheticArchiveData(int size) {
        byte[] data = new byte[size];
        Random random = new Random();
        random.nextBytes(data);
        // Simulate ZIP header
        data[0] = 0x50; data[1] = 0x4B; data[2] = 0x03; data[3] = 0x04; // ZIP header
        return data;
    }

    private static String generateXmlReport(int targetSize) {
        StringBuilder sb = new StringBuilder();
        sb.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<report>\n");

        while (sb.length() < targetSize) {
            sb.append("  <entry id=\"").append((int)(Math.random() * 10000)).append("\">\n");
            sb.append("    <timestamp>").append(System.currentTimeMillis()).append("</timestamp>\n");
            sb.append("    <value>").append(Math.random() * 1000).append("</value>\n");
            sb.append("    <category>sales</category>\n");
            sb.append("  </entry>\n");
        }
        sb.append("</report>");
        return sb.toString();
    }

    private static String generateJsonAnalytics(int targetSize) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"analytics\": [\n");

        boolean first = true;
        while (sb.length() < targetSize) {
            if (!first) sb.append(",\n");
            sb.append("  {\"userId\": ").append((int)(Math.random() * 1000))
              .append(", \"action\": \"page_view\", \"timestamp\": ").append(System.currentTimeMillis())
              .append(", \"duration\": ").append((int)(Math.random() * 300)).append("}");
            first = false;
        }
        sb.append("\n]}");
        return sb.toString();
    }

    private static String generateCsvDataSet(int targetSize) {
        StringBuilder sb = new StringBuilder();
        sb.append("id,timestamp,user_id,transaction_amount,status\n");

        while (sb.length() < targetSize) {
            sb.append((int)(Math.random() * 100000)).append(",")
              .append(System.currentTimeMillis()).append(",")
              .append((int)(Math.random() * 1000)).append(",")
              .append(String.format("%.2f", Math.random() * 1000)).append(",")
              .append(Math.random() > 0.1 ? "completed" : "failed").append("\n");
        }
        return sb.toString();
    }

    // Utility methods
    private static double getCompressionRatio(DocumentInfo doc) {
        return ((double)(doc.originalSize - doc.compressedSize) / doc.originalSize) * 100;
    }

    private static double getCompressionRatio(long originalSize, long compressedSize) {
        return ((double)(originalSize - compressedSize) / originalSize) * 100;
    }

    private static String formatBytes(long bytes) {
        if (bytes < 1024) return bytes + " B";
        if (bytes < 1024 * 1024) return String.format("%.1f KB", bytes / 1024.0);
        if (bytes < 1024 * 1024 * 1024) return String.format("%.1f MB", bytes / (1024.0 * 1024.0));
        return String.format("%.1f GB", bytes / (1024.0 * 1024.0 * 1024.0));
    }

    // Data classes
    static class DocumentInfo {
        String metadata;
        byte[] content;
        long originalSize;
        long compressedSize;

        DocumentInfo(String metadata, byte[] content, long originalSize, long compressedSize) {
            this.metadata = metadata;
            this.content = content;
            this.originalSize = originalSize;
            this.compressedSize = compressedSize;
        }
    }

    static class FileInfo {
        String filename;
        String mimeType;
        byte[] fileData;
        long originalSize;
        long compressedSize;
        long uploadTimestamp;

        FileInfo(String filename, String mimeType, byte[] fileData, long originalSize, long compressedSize, long uploadTimestamp) {
            this.filename = filename;
            this.mimeType = mimeType;
            this.fileData = fileData;
            this.originalSize = originalSize;
            this.compressedSize = compressedSize;
            this.uploadTimestamp = uploadTimestamp;
        }
    }

    static class CompressedDataInfo {
        String dataType;
        byte[] compressedData;
        long dataSize;
        String compressionType;

        CompressedDataInfo(String dataType, byte[] compressedData, long dataSize, String compressionType) {
            this.dataType = dataType;
            this.compressedData = compressedData;
            this.dataSize = dataSize;
            this.compressionType = compressionType;
        }
    }
}
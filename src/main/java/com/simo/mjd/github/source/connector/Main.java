package com.simo.mjd.github.source.connector;

/**
 * Informational entry point for the Kafka Connect GitHub Source Connector plugin JAR.
 *
 * <p>Kafka Connect discovers connector plugins through {@code META-INF/services}
 * service provider configuration, rather than an executable {@code main} method.
 * This class provides a friendly message if the fat JAR is executed directly
 * via {@code java -jar}.
 */
public class Main {

    /**
     * Prints connector metadata and deployment instructions.
     *
     * @param args command line arguments (unused)
     */
    public static void main(String[] args) {
        System.out.println("===============================================================================");
        System.out.println("Kafka Connect GitHub Source Connector Plugin");
        System.out.println("===============================================================================");
        System.out.println("Connector Class : com.simo.mjd.github.source.connector.source.GitHubSourceConnector");
        System.out.println("Task Class      : com.simo.mjd.github.source.connector.source.GitHubSourceTask");
        System.out.println("Deployment      : Place this JAR in Kafka Connect's 'plugin.path' directory.");
        System.out.println("===============================================================================");
    }
}

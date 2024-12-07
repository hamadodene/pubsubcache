package org.pubsubcache;

import org.apache.pulsar.client.api.PulsarClientException;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.servlet.ServletContainer;
import org.pubsubcache.api.CacheApplication;
import org.pubsubcache.cache.CacheService;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.glassfish.jersey.servlet.ServletProperties.JAXRS_APPLICATION_CLASS;

public class Main {
    private static final Logger logger = Logger.getLogger(Main.class.getName());

    public static void main(String[] args) {
        try {
            // Load properties for testing purposes
            // User can pass a custom properties file path as the first argument
            String customPropertiesPath = args.length > 0 ? args[0] : null;
            loadProperties(customPropertiesPath);

            new CacheService();

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    private static void loadProperties(String customPropertiesPath) {
        Properties prop = new Properties();

        try (InputStream input = getPropertiesInputStream(customPropertiesPath)) {
            if (input != null) {
                prop.load(input);
                prop.forEach((key, value) -> {
                    if (System.getProperty((String) key) == null) {
                        System.setProperty((String) key, (String) value);
                    }
                });
                logger.log(Level.INFO,"Properties loaded successfully.");
            } else {
                logger.log(Level.WARNING,"No properties file found. Continuing with system properties.");
            }
        } catch (IOException ex) {
            throw new RuntimeException("Failed to load properties file", ex);
        }
    }
    private static InputStream getPropertiesInputStream(String customPath) throws IOException {
        if (customPath != null) {
            Path path = Path.of(customPath);
            if (Files.exists(path)) {
                logger.log(Level.WARNING, "Loading properties from custom path: " + customPath);
                return Files.newInputStream(path);
            }
        }
        return null;
    }
}

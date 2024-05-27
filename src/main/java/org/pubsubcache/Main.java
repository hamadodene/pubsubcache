package org.pubsubcache;

import org.apache.pulsar.client.api.PulsarClientException;
import org.pubsubcache.cache.CacheService;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.logging.Logger;

public class Main {
    private static final java.util.logging.Logger logger = Logger.getLogger(Main.class.getName());

    public static void main(String[] args) {
        try {
            //load properties for testing purpose
            // You can in general set properties like -Dnode.id=node1
            // Or you can set to system properties
            loadProperties();
            CacheService cacheService = new CacheService();

            // Start listener for incoming Pulsar messages
            cacheService.startListener();
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    private static void loadProperties() {
        try (InputStream input = Main.class.getClassLoader().getResourceAsStream("conf/application.properties")) {
            if (input == null) {
                System.out.println("Sorry, unable to find application.properties");
                return;
            }

            Properties prop = new Properties();
            prop.load(input);

            prop.forEach((key, value) -> System.setProperty((String) key, (String) value));
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }
}
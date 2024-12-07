FROM eclipse-temurin:17

# Add the JAR file and the shell script to the container
ADD target/pubsubcache*.jar pubsubcache.jar
ADD start.sh /start.sh

# Make the script executable
RUN chmod +x /start.sh

# Use the shell script to start the application
ENTRYPOINT ["/start.sh"]

# Expose port 8080
EXPOSE 8080

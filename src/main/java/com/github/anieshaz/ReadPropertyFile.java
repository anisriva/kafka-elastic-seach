package com.github.anieshaz;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

public class ReadPropertyFile {

    // app vars
    public boolean APP_ALWAYS_ON;
    public String KEY_TAG;

    // Broker vars
    public String TOPIC;
    public String KAFKA_BOOTSTRAP_SERVER;
    public String PRODUCER_COMPRESSION_TYPE;
    public int PRODUCER_BATCH_SIZE;
    public int PRODUCER_LINGER_MS;
    public int CONSUMER_POLL_TIMEOUT_MS;
    public int CONSUMER_MAX_POLL_RECORDS;
    public boolean CONSUMER_COMMIT_ASYNC;

    // twitter vars
    public int TWITTER_QUEUE_CAPACITY;
    public int TWITTER_POLL_SECONDS;
    public String TWITTER_CONSUMER_API_KEY;
    public String TWITTER_CONSUMER_API_SECRET;
    public String TWITTER_APP_TOKEN;
    public String TWITTER_APP_SECRET;

    // ES vars
    public String ES_HOSTNAME;
    public int ES_PORT;
    public String ES_SCHEME;
    public String ES_INDEX;

    public Logger logger;

    public ReadPropertyFile(String fileName, String className){

        logger = LoggerFactory.getLogger(className);

        Properties properties = null;

        try{
            properties = loadConfig(fileName);
        }catch (IOException e){
            logger.error("Caught Exception : "+ Arrays.toString(e.getStackTrace()));
            System.exit(1);
        }

        // app vars
        APP_ALWAYS_ON = Boolean.parseBoolean(properties.getProperty("app.always.on"));

        // twitter vars
        TWITTER_QUEUE_CAPACITY = Integer.parseInt(properties.getProperty("twitter.queue.capacity"));
        TWITTER_POLL_SECONDS = Integer.parseInt(properties.getProperty("twitter.poll.timeout.seconds"));
        TWITTER_CONSUMER_API_KEY = properties.getProperty("twitter.consumer.api.key");
        TWITTER_CONSUMER_API_SECRET = properties.getProperty("twitter.consumer.api.secret");
        TWITTER_APP_TOKEN = properties.getProperty("twitter.app.token");
        TWITTER_APP_SECRET = properties.getProperty("twitter.app.secret");

        // kafka vars
        KAFKA_BOOTSTRAP_SERVER = properties.getProperty("kafka.bootstrap.server");
        PRODUCER_COMPRESSION_TYPE = properties.getProperty("producer.msg.compression.type");
        PRODUCER_BATCH_SIZE = Integer.parseInt(properties.getProperty("producer.batch.size.kb"));
        PRODUCER_LINGER_MS = Integer.parseInt(properties.getProperty("producer.linger.ms"));
        CONSUMER_POLL_TIMEOUT_MS = Integer.parseInt(properties.getProperty("consumer.poll.timeout.ms"));
        CONSUMER_MAX_POLL_RECORDS = Integer.parseInt(properties.getProperty("consumer.max.poll.records"));
        CONSUMER_COMMIT_ASYNC = Boolean.parseBoolean(properties.getProperty("consumer.commit.async"));

        // es vars
        ES_HOSTNAME = properties.getProperty("es.hostname");
        ES_PORT = Integer.parseInt(properties.getProperty("es.port"));
        ES_SCHEME = properties.getProperty("es.scheme");
        ES_INDEX = properties.getProperty("es.index");
    }


    public Properties loadConfig(String fileName) throws IOException {
        Properties appProps = new Properties();
        FileInputStream configFile = null;
        try{
            configFile = new FileInputStream(fileName);
        }catch (FileNotFoundException e){
            logger.error(String.format("Unable to find %s, application will be terminated ...", fileName));
            System.exit(1);
        }
        appProps.load(configFile);
        return appProps;
    }

}

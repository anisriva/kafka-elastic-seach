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
    public String BOOTSTRAP_SERVER;
    public String COMPRESSION_TYPE;
    public int BATCH_SIZE;
    public int LINGER_MS;
    public int CONSUMER_POLL_TIMEOUT_MS;
    public boolean COMMIT_ASYNC;

    // twitter vars
    public int QUEUE_CAPACITY;
    public int POLL_SECONDS;
    public String CONSUMER_API_KEY;
    public String CONSUMER_API_SECRET;
    public String APP_TOKEN;
    public String APP_SECRET;

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
        QUEUE_CAPACITY = Integer.parseInt(properties.getProperty("queue.capacity"));
        POLL_SECONDS = Integer.parseInt(properties.getProperty("poll.timeout.seconds"));
        CONSUMER_API_KEY = properties.getProperty("consumer.api.key");
        CONSUMER_API_SECRET = properties.getProperty("consumer.api.secret");
        APP_TOKEN = properties.getProperty("twitter.app.token");
        APP_SECRET = properties.getProperty("twitter.app.secret");

        // kafka vars
        BOOTSTRAP_SERVER = properties.getProperty("bootstrap.server");
        COMPRESSION_TYPE = properties.getProperty("msg.compression.type");
        BATCH_SIZE = Integer.parseInt(properties.getProperty("batch.size.kb"));
        LINGER_MS = Integer.parseInt(properties.getProperty("linger.ms"));
        CONSUMER_POLL_TIMEOUT_MS = Integer.parseInt(properties.getProperty("consumer.poll.timeout.ms"));
        COMMIT_ASYNC = Boolean.parseBoolean(properties.getProperty("consumer.commit.async"));

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

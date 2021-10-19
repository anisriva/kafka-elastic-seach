package com.github.anieshaz;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer extends ReadPropertyFile{

    public TwitterProducer(){
        super("src/main/conf/config.properties", TwitterProducer.class.getName());
    }

    public KafkaProducer<String, String> createKafkaProducer(String bootstrapServer) {
        // create config
        Properties producerConfig = new Properties();
        producerConfig.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        producerConfig.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerConfig.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Config for idempotent producer for safe production of messages
        producerConfig.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        producerConfig.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        producerConfig.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));

        // config for compression to minimize latency and improve throughput
        producerConfig.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, COMPRESSION_TYPE);
        producerConfig.setProperty(ProducerConfig.LINGER_MS_CONFIG, Integer.toString(LINGER_MS));
        producerConfig.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(BATCH_SIZE*1024)); // 32 KB batch size

        printProperties(producerConfig);
        // create and return producer
        return new KafkaProducer<>(producerConfig);
    }
    public static void printProperties(Properties prop)
    {
        for (Object key: prop.keySet()) {
            System.out.println(key + ": " + prop.getProperty(key.toString()));
        }
    }

    public Client createTwitterClient(
            BlockingQueue<String> msgQueue,
            List<String> terms,
            String twitterClientName) {

        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(CONSUMER_API_KEY, CONSUMER_API_SECRET, APP_TOKEN, APP_SECRET);

        ClientBuilder builder = new ClientBuilder()
                .name(twitterClientName)
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        return builder.build();
    }

    public void run() {

        // search tags
        Scanner getInput = new Scanner(System.in);
        System.out.println("Enter the search keyword : ");
        KEY_TAG = getInput.nextLine();

        List<String> terms = Lists.newArrayList(KEY_TAG);

        // twitter client name
        String twitterClientName = ES_INDEX+"-client-"+KEY_TAG;

        // topic name
        TOPIC = ES_INDEX+"."+KEY_TAG;

        // twitter client
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(QUEUE_CAPACITY);
        Client client = createTwitterClient(msgQueue, terms, twitterClientName);
        client.connect();

        // kafka producer
        KafkaProducer<String, String> producer = createKafkaProducer(BOOTSTRAP_SERVER);

        // create a shutdown hook
        Runtime.getRuntime().addShutdownHook( new Thread(() ->{
            logger.info("Shutting down "+ twitterClientName);
            client.stop();
            logger.info("Shutting down producer");
            producer.close();
        }));

        // loop to send tweets to kafka
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(QUEUE_CAPACITY, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                logger.warn("Caught [ InterruptedException ] terminating execution ... ");
                client.stop();
            }
            if (msg != null) {
                logger.debug(msg);
                producer.send(new ProducerRecord<>(TOPIC, null, msg), (recordMetadata, e) -> {
                    if (e!=null){
                        logger.error(String.format("Caught exception while producing the message to topic [%s] : %s",TOPIC, Arrays.toString(e.getStackTrace())));
                    }else{
                        logger.info(String.format("Message pushed to topic : [%s]", TOPIC));
                    }
                });
            }else if (!APP_ALWAYS_ON) break;
        }

        logger.info("End of app");
    }

    public static void main(String[] args) {

        new TwitterProducer().run();

    }

}
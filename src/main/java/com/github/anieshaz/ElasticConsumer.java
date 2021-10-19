package com.github.anieshaz;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.*;
import java.io.IOException;
import java.time.Duration;


public class ElasticConsumer extends TwitterProducer {

    public RestHighLevelClient createElasticClient(){
        logger.info("Building Elastic Search client");
        return new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost(ES_HOSTNAME, ES_PORT, ES_SCHEME)));
    }

    public KafkaConsumer<String, String> createConsumer(){
        // get the properties
        Properties consumerProperties = new Properties();
        // basic props
        consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        consumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // group properties
        consumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "grp_"+TOPIC.replace(".","_"));
        // auto-commit
        consumerProperties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");


        // create consumer
        return new KafkaConsumer<>(consumerProperties);

    }

    public String extractIDfromRecord(ConsumerRecord<String, String> record){
        JsonParser parser = new JsonParser();
        String id = "";
        try{
            id = parser.parse(record.value())
                    .getAsJsonObject()
                    .get("id")
                    .getAsString();
        }catch (NullPointerException e){
            logger.info("Caught NullPointerException for : " + record.value());
        }
        return id;
    }

    public void run(){
        // tag
        String tag = String.format("%s_%s",ES_INDEX,KEY_TAG);

        // create elastic client
        logger.info("Creating ES client with tag "+ tag);
        RestHighLevelClient client = createElasticClient();
        IndexRequest indexRequest = new IndexRequest(tag);

        // fetch data
        if (TOPIC == null){
            logger.warn("TOPIC not found");
            System.out.println("Provide the name of the topic to be consumed");
            Scanner sc = new Scanner(System.in);
            TOPIC = sc.nextLine();
        }

        // create consumer
        logger.info("Creating consumer");
        KafkaConsumer<String, String> consumer = createConsumer();

        logger.info("Subscribing to :"+TOPIC);
        consumer.subscribe(Collections.singletonList(TOPIC));

        boolean keepConsuming = true;

        Runtime.getRuntime().addShutdownHook(new Thread(() ->{
            logger.info("Shutting down the consumer");
            try{
                consumer.close();
            }catch (ConcurrentModificationException e){
                logger.warn("Caught exception [ ConcurrentModificationException ] while closing : "+e.getMessage());
            }
            logger.info("Shutting down the ES client");
            try {
                client.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }));
        int totalRecordCount=0;

        while(keepConsuming){
            ConsumerRecords<String, String> records =  consumer.poll(Duration.ofMillis(CONSUMER_POLL_TIMEOUT_MS));
            int batchRecordCount=0;
            for (ConsumerRecord<String, String> record : records){

                batchRecordCount++;
                totalRecordCount++;

                logger.info(String.format("Received record [%s] / [%s] for topic [%s] total rows processed [%s]",batchRecordCount,records.count(),record.topic(), totalRecordCount));

                // send data
                if (Objects.equals(extractIDfromRecord(record), "")){
                    logger.warn("Skipping record without an id");
                    continue;
                }
                indexRequest.id(extractIDfromRecord(record));
                indexRequest.source(record.value(), XContentType.JSON);
                IndexResponse indexResponse;
                String responseId = "";
                try {
                    indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
                    responseId = indexResponse.getId();
                } catch (IOException e) {
                    logger.error("Caught IOException");
                }catch (ElasticsearchStatusException e){
                    logger.warn("Caught ElasticsearchStatusException while processing message will be skipped ... : "+e.getMessage());
                }

                if (batchRecordCount==records.count() && !APP_ALWAYS_ON){
                    keepConsuming=false;
                    logger.info("All records fetched exiting the consumer ... ");
                    break;
                }else if(Objects.equals(responseId, "")){
                    logger.warn("Skipping record without id");
                }else{
                    logger.info(String.format("Message pushed in ES with id : [%s] committing the offset in the broker with consumer.commit.async set as [%b]",
                            responseId,
                            COMMIT_ASYNC));
                    if (COMMIT_ASYNC) {
                        consumer.commitAsync();
                    } else consumer.commitSync();
                }
            }
        }
        try {
            client.close();
        } catch (IOException e) {
            logger.error("Error occurred while closing the connection : "+ e.getMessage());
        }

    }

    public static void main(String[] args) {

        new ElasticConsumer().run();
    }
}

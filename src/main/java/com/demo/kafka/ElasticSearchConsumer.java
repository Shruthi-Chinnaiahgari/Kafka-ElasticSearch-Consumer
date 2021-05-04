package com.demo.kafka;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonParser;


public class ElasticSearchConsumer {

	static Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class);

	public static RestHighLevelClient createClient() {
		
		String hostname = "kafka-shruthi-5534640723.eu-west-1.bonsaisearch.net";
		String username = "4xnplg3qog";
		String password = "h102vfmgbr";
		
		final CredentialsProvider credentialProvider = new BasicCredentialsProvider();
		credentialProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
		
		RestClientBuilder builder = RestClient.builder(new HttpHost (hostname, 443, "https")).setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
			
			public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
				// TODO Auto-generated method stub
				return httpClientBuilder.setDefaultCredentialsProvider(credentialProvider);
			}
		});
				
		RestHighLevelClient client = new RestHighLevelClient(builder);
		return client;
		
	}
	
	public static KafkaConsumer<String, String> createKafkaConsumer(){
		//create consumer properties
		Properties properties = new Properties();
		String bootStrapServers = "127.0.0.1:9092";
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // disable auto commit of offset
		properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100"); // Will fetch 10 records per poll

		//"earliest" -- prints messages from beginning
		//"latest" -- prints latest messages
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		//We can add group id also to consumer properties
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "twitter_imp_tweets_group");

		//create consumer
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
		consumer.subscribe(Arrays.asList("ImportantTweets"));
		return consumer;

	}
	
	public static void main(String[] args) throws IOException {

		RestHighLevelClient  client = createClient();

		//String jsonString = "{ \"name\" : \"shruthi\" }";


		KafkaConsumer<String, String> consumer = createKafkaConsumer();

		//poll for new data
		while(true) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
			logger.info("Received count " + records.count());
			BulkRequest bulkRequest = new BulkRequest();
			if(records.count() > 1) {
				for(ConsumerRecord<String, String> record : records) {

					//2 startegies
					//kafka generic id
					//String id = record.topic() + record.partition() + record.offset();

					//twitter specific id
					String id = extractTwiterFeedId(record.value());
					logger.info("id " + id);

					//Insert data into elastic search
					//Sending id to make it as idempotent consumers
					IndexRequest request = new IndexRequest("imp-twitter", "tweets", id).source(record.value(), XContentType.JSON);
					bulkRequest.add(request);
					//IndexResponse response = client.index(request, RequestOptions.DEFAULT);
					//String elasticSeachId  = response.getId();
					//logger.info(elasticSeachId);
				}
				BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
				logger.info("Committig offsets");
				consumer.commitAsync();
				logger.info("Offsets have been committed");
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}

		//client.close();

	}
	private static JsonParser parser = new JsonParser();
	
	private static String extractTwiterFeedId(String value) {
		return parser.parse(value).getAsJsonObject().get("id_str").getAsString();
	}

}

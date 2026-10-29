package com.wikimediaproducer;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import okhttp3.Headers;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaChangesProducer {

    public static void main(String[] args) throws Exception {

        // 1. Create producer properties
        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.20.215.192:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 2. Create Kafka producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        String topic = "wikimedia.recentchange";

        // 3. Create the event handler
        EventHandler eventHandler = new WikimediaChangeHandler(producer, topic);

        // 4. Wikimedia stream URL

        String url = "https://stream.wikimedia.org/v2/stream/recentchange";

        Headers headers = new Headers.Builder()
                .add("User-Agent",
                        "WikimediaKafkaProducer/1.0 (contact: your-real-email@example.com)")
                .build();

        EventSource eventSource = new EventSource.Builder(eventHandler, URI.create(url)).headers(headers).build();

        try {
            // 5. Start receiving Wikimedia events
            eventSource.start();

            // 6. Run for 10 minutes
            TimeUnit.MINUTES.sleep(10);

        } finally {
            // 7. Clean up resources
            eventSource.close();
            producer.close();
        }
    }
}

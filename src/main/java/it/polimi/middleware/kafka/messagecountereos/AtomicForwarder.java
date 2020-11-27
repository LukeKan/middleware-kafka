package it.polimi.middleware.kafka.messagecountereos;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;

public class AtomicForwarder {
    private static final String defaultConsumerGroupId = "groupA";
    private static final String defaultInputTopic = "msg";
    private static final String defaultOutputTopic = "counter";

    private static final String serverAddr = "localhost:9092";
    private static final String producerTransactionalId = "forwarderTransactionalId";
    private static final boolean autoCommit = true;
    private static final int autoCommitIntervalMs = 15000;

    // Default is LATEST: try EARLIEST instead
    private static final String offsetResetStrategy = "latest";

    public static void main(String[] args) {
        // If there are arguments, use the first as group, the second as input topic, the third as output topic.
        // Otherwise, use default group and topics.
        String consumerGroupId = args.length >= 1 ? args[0] : defaultConsumerGroupId;
        String inputTopic = args.length >= 2 ? args[1] : defaultInputTopic;
        String outputTopic = args.length >= 3 ? args[2] : defaultOutputTopic;

        // Consumer
        final Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);

        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        // The consumer does not commit automatically, but within the producer transaction
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(false));

        final Properties initProps = new Properties();
        initProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
        initProps.put(ConsumerConfig.GROUP_ID_CONFIG, defaultConsumerGroupId);
        initProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(autoCommit));
        initProps.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, String.valueOf(autoCommitIntervalMs));

        initProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetResetStrategy);

        initProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        initProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());


        KafkaConsumer<String, String> synchronizer = new KafkaConsumer<>(initProps);
        synchronizer.subscribe(Collections.singletonList(outputTopic));

        HashMap<String,Integer> state = new HashMap<>();
        for (final ConsumerRecord<String, String> record : synchronizer.poll(Duration.of(5, ChronoUnit.MINUTES))) {
            System.out.println("Partition: " + record.partition() +
                    "\tOffset: " + record.offset() +
                    "\tKey: " + record.key() +
                    "\tValue: " + record.value()
            );
            state.putIfAbsent(record.key(),Integer.parseInt(record.value()));
        }
        synchronizer.close();

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList(inputTopic));

        // Producer
        final Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, producerTransactionalId);
        producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, String.valueOf(true));

        final KafkaProducer<String,String> producer = new KafkaProducer<String,String>(producerProps);
        producer.initTransactions();

        while (true) {
            final ConsumerRecords<String, String> records = consumer.poll(Duration.of(5, ChronoUnit.MINUTES));
            producer.beginTransaction();
            for (final ConsumerRecord<String, String> record : records) {
                state.putIfAbsent(record.key(),0);
                state.put(record.key(), state.get(record.key())+1);
                System.out.println(
                        "Key: " + record.key() +
                                "\tCounter: " + state.get(record.key())
                );
                state.forEach((key,value)->{
                    producer.send(new ProducerRecord<String, String>(outputTopic, key, value.toString()));

                });


            }
            producer.commitTransaction();
        }
    }
}
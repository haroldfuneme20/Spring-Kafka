package com.hhfm.kafkaspring;

import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaProducerException;
import org.springframework.kafka.core.KafkaSendCallback;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.util.concurrent.ListenableFuture;

import java.util.List;

@SpringBootApplication
@EnableScheduling
public class KafkaSpringApplication implements CommandLineRunner {
    // producer template
    @Autowired
    private  KafkaTemplate<String,String> kafkaTemplate;
    @Autowired
    private KafkaListenerEndpointRegistry registry;

    @Autowired
    private MeterRegistry meterRegistry;


    public static final Logger log = LoggerFactory.getLogger(KafkaSpringApplication.class);

    public static void main(String[] args) {
        SpringApplication.run(KafkaSpringApplication.class, args);

    }


    // listener consumer
   // @KafkaListener(topics = "topic-test", containerFactory = "listenerContainerFactory", groupId = "hhfm-group-id", properties = {"max.poll.interval.ms:4000", "max.poll.records:10"})
    public void listen(List<String> messages){
        log.info("Start reading msj");
        for (String m :messages) {
        log.info("Message: {} ", m);
        }
        log.info("End Batch reading msj");
    }

    // listener consumer
    @KafkaListener(id = "hhfm", autoStartup ="true", topics = "topic-test", containerFactory = "listenerContainerFactory", groupId = "hhfm-group-id",
            properties = {"max.poll.interval.ms:4000", "max.poll.records:50"})
    public void listenFull(List<ConsumerRecord<String, String>> messages){
        log.info("Start reading msj");
        for (ConsumerRecord<String, String> m :messages) {
//            log.info("Partition: {} ", m.partition());
//            log.info("Offset: {} ", m.offset());
//            log.info("Key: {} ", m.key());
//            log.info("Value: {} ", m.value());
        }
        log.info("End Batch reading msj");
    }

    // producer template
    @Override
    public void run(String... args) throws Exception {

//            Thread.sleep(5000);
//            registry.getListenerContainer("hhfm").start();

//            ListenableFuture<SendResult<String,String>> future = kafkaTemplate.send("topic-test", "Sample message");
//            //kafkaTemplate.send("topic-test", "Sample message").get();// SYNC
//            future.addCallback(new KafkaSendCallback<String,String>(){
//                @Override
//                public void onSuccess(SendResult<String, String> result) {
//                    log.info("Message Sent", result.getRecordMetadata().offset());
//                }
//
//                @Override
//                public void onFailure(KafkaProducerException e) {
//                    log.error("Message error", e.getFailedProducerRecord());
//                }
//            });
    }


    // producer
    @Scheduled(fixedDelay = 2000, initialDelay = 500)
    public void sendMsjToKafka(){
        log.info("SEND msj to KAFKA");
        for (int i = 0; i < 100; i++) {
            kafkaTemplate.send("topic-test", String.valueOf(i), String.format("Sample msj %d", i));
        }

    }

    @Scheduled(fixedDelay = 2000, initialDelay = 500)
    public void messageCountMetric() {
        double count = meterRegistry.get("kafka.producer.record.send.total").functionCounter().count();
        log.info("Count {} ",count);
    }

    
}

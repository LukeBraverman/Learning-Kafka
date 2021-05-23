package com.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallBack {
    public static void main(String[] args) {
      Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallBack.class);


      String test = "1more";
        String bootstrapServers = "127.0.0.1:9092";
        //create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // create a producer record
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", test);


        //send data
        producer.send(record, new Callback() {
          @Override
          public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            //Executes everytime a record is sent OR a exception is thrown
            if (e == null) {
              //record was sent
              logger.info("Recieved new metadate . \n" +
                          "Topic: " + recordMetadata.topic() +"\n"
                          +"Partition: " + recordMetadata.partition() + "\n"
                          +"Offset: " +recordMetadata.offset() + "\n");
            } else {
              logger.error("Error while producing " + e);
            }


          }
        });

        //flush data
        producer.flush();

        //flush and close
        producer.close();
    }
}

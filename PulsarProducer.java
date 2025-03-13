package com.example;

import org.apache.pulsar.client.api.PulsarClient;

//import java.util.concurrent.CompletableFuture;

import org.apache.pulsar.client.api.Producer;
//import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.Schema;
//import org.apache.pulsar.client.api.schema.GenericRecord;
//import org.apache.pulsar.common.schema.SchemaType;
//import org.json.JSONObject;
import org.apache.pulsar.client.api.PulsarClientException;

//import java.util.concurrent.TimeUnit;


public class PulsarProducer{

        //private final Producer<String> producer = null;
        private final Producer<PulsarJsonSchema> producer = null;

        public void CreateSender (String serviceUrl, String topicName) throws PulsarClientException {

        // Step 1: Create a Pulsar client
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(serviceUrl)
                .build();

        // Step 2: Create a producer
        @SuppressWarnings("unused")
        Producer<PulsarJsonSchema> producer = client.newProducer(Schema.JSON(PulsarJsonSchema.class))
                .topic(topicName)
                .create();
        }

        public void Send (String message) throws PulsarClientException {

                // Create an instance of JSON class
                PulsarJsonSchema myJson = new PulsarJsonSchema();
                JsonConvert jsvalue = new JsonConvert();
                myJson.setSensorID(jsvalue.getSensorIdValue(message));
                myJson.setCoordinate(jsvalue.getCoordinateValue(message));
                myJson.setStatus(jsvalue.getStatusValue(message));
                

                // Send the message
                producer.newMessage().value(myJson).send();

                // Close the producer and client
                //producer.close();
                //client.close();
        }

        public void CloseSender() throws PulsarClientException {
                producer.close();
        }
}

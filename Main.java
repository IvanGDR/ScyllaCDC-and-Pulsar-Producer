package com.example;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

//import com.example.TestCDCConsumer;

import org.apache.pulsar.client.api.PulsarClientException;


public class Main {
    public static void main(String[] args) throws InterruptedException, ExecutionException, PulsarClientException {

        System.out.println("Starting ScyllaCDC and Pulsar Producer");  // reached

        //Create Producer
        // Get Configuring values
        GetConfigValue cpvalue = new GetConfigValue();
        String serviceUrl = cpvalue.GetPulsarServiceURL();
        String topicName = cpvalue.GetTopicName();

        PulsarProducer pulsarproducer = new PulsarProducer();
        pulsarproducer.CreateSender(serviceUrl, topicName);

        System.out.println("Pulsar Client connected and Topic Created"); // reached, an in my Pulsar Cluster topic gets created


        // Produce the message
        // Get cdc log as a string. One record at a time so this can be passed to the producer sender
        TestCDCConsumer cdc = new TestCDCConsumer();
        CompletableFuture<String> cdcmessage = cdc.CDCBuilder();
        String message = cdcmessage.get();

        /*
        From below println, I expect for example:
        my message from CDC -> {"coordinate":733,"sensor_id":"sensor733","status":"ACTIVE"}
        What I get printed is:
        ....
        {"coordinate":253,"sensor_id":"sensor253","status":"ACTIVE"}
        {"coordinate":260,"sensor_id":"sensor260","status":"ACTIVE"}
        {"coordinate":270,"sensor_id":"sensor270","status":"ACTIVE"}
        ...

        */

        System.out.println("my message from CDC as string-> " + message); // I do not get it printed :(
          
        //while message is not null produce the message
        while (true) {
            if (message != null) {
                //PulsarProducer pulsarProducer = new PulsarProducer();
                //System.out.println("Producing" + message);
                pulsarproducer.Send(message);
            }
        }
    }
}

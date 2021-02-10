package com.poc.kafkabeginner;

import com.poc.kafkabeginner.consumer.ConsumerDemo;
import com.poc.kafkabeginner.producer.ProducerDemo;
import com.poc.kafkabeginner.producer.ProducerDemoKey;
import com.poc.kafkabeginner.producer.ProducerDemoWithCallback;
import org.springframework.boot.autoconfigure.SpringBootApplication;

//@SpringBootApplication
public class KafkaBeginnerApplication {

    public static void main(String[] args) {
    	//SpringApplication.run(KafkaBeginnerApplication.class, args);
//		ProducerDemo producerDemo = new ProducerDemo();
//		producerDemo.sendData();
//
//		ProducerDemoWithCallback demoWithCallback = new ProducerDemoWithCallback();
//		demoWithCallback.sendData();

//		ProducerDemoKey producerDemoKey = new ProducerDemoKey();
//		producerDemoKey.sendData();

		ConsumerDemo consumerDemo = new ConsumerDemo();
		consumerDemo.pollData();

	}

}

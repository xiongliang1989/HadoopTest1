package com.xl.kafka.producer;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;

@SuppressWarnings("deprecation")
public class KafkaProducer extends Thread {

	private String topic;

	public KafkaProducer(String topic) {
		super();
		this.topic = topic;
	}

	@Override
	public void run() {
		Producer<Integer, String> producer = createProducer();
		int i = 0;
		while (true) {
			producer.send(new KeyedMessage<Integer, String>(topic, "message: " + i++));
			try {
				TimeUnit.SECONDS.sleep(1);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	private Producer<Integer, String> createProducer() {
		Properties properties = new Properties();
		properties.put("zookeeper.connect", "localhost:2181");// 声明zk
		properties.put("serializer.class", StringEncoder.class.getName());
		properties.put("metadata.broker.list", "127.0.0.1:9092");// 声明kafka
																											// broker
		return new Producer<Integer, String>(new ProducerConfig(properties));
	}

	public static void main(String[] args) {
		new KafkaProducer("test").start();// 使用kafka集群中创建好的主题 test

	}

}

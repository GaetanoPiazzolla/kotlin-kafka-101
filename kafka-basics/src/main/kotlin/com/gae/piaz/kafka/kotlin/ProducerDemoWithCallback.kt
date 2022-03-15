package com.gae.piaz.kafka.kotlin

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import java.util.*

class ProducerDemoWithCallback {

    companion object {
        @Suppress("JAVA_CLASS_ON_COMPANION")
        @JvmStatic
        private val logger = LoggerFactory.getLogger(javaClass.enclosingClass)
    }

    private var prop: Properties = Properties()
    private var producer: KafkaProducer<String, String>

    init {
        prop[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = "127.0.0.1:9092"
        prop[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
        prop[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
        producer = KafkaProducer(prop)
    }

    fun send(message: String) {
        val record = ProducerRecord<String, String>("app_topic", message)

        this.producer.send(record) { recordMetadata: RecordMetadata, e: Exception? ->
            if (e == null) {
                logger.info(
                    "received new metadata: \n" +
                            "topic: ${recordMetadata.topic()} \n" +
                            "partition: ${recordMetadata.partition()} \n" +
                            "offset: ${recordMetadata.offset()} \n" +
                            "timestamp: ${recordMetadata.timestamp()} \n"
                )
            } else {
                logger.error("Error while producing", e)
            }
        }

    }

    fun sendData() {
        this.producer.flush()
    }

}

fun main(args: Array<String>) {

    val producer = ProducerDemoWithCallback()

    for(i in 0..10) {
        producer.send("$i ciao")
    }

    producer.sendData()

}
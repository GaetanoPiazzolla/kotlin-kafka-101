package com.gae.piaz.kafka.kotlin

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import java.util.*

class ProducerDemoKeys {

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

    fun send(message: String, key: String) {

        val record = ProducerRecord("app_topic", message, key)

        logger.info("key: $key")
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
        }.get() // FIXME block the .send to make it synch (so we can match the key with the partition! -> NO PROD)

    }

    fun sendData() {
        this.producer.flush()
    }

}

fun main(args: Array<String>) {

    val producer = ProducerDemoKeys()

    for (i in 1..10) {
        producer.send("$i ciao", "id_$i")
    }

    producer.sendData()

}
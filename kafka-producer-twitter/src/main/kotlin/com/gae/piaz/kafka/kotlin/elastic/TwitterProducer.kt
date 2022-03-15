package com.gae.piaz.kafka.kotlin.elastic

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import java.util.*

class TwitterProducer(twitterClient: TwitterClient, safe: Boolean, highThroughput: Boolean) {

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

        if (safe) {
            prop[ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG] = "true"
            prop[ProducerConfig.ACKS_CONFIG] = "all"
            prop[ProducerConfig.RETRIES_CONFIG] = Int.MAX_VALUE.toString()
            prop[ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION] = "5"
        }

        if (highThroughput) {
            prop[ProducerConfig.COMPRESSION_TYPE_CONFIG] = "snappy"
            prop[ProducerConfig.LINGER_MS_CONFIG] = "20"
            prop[ProducerConfig.BATCH_SIZE_CONFIG] = (32 * 1024).toString() // 32 KB
        }

        producer = KafkaProducer(prop)

        var messagesSent = 0
        while (!twitterClient.isDone()) {
            twitterClient.takeMessage()?.let {
                send(it)
                messagesSent++
            }
            if (messagesSent % 10 == 0) {
                logger.info("flushing producer data")
                this.producer.flush()
            }
        }
        this.producer.flush()
    }

    fun close() {
        this.producer.close()
    }

    private fun send(message: String) {
        val record = ProducerRecord<String, String>("twitter_tweets", message)

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

}


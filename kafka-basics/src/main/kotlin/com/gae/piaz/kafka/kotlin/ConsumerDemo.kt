package com.gae.piaz.kafka.kotlin

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.temporal.ChronoUnit
import java.util.*

class ConsumerDemo(private val topics: List<String>?, private val groupId: String?) {

    companion object {
        @Suppress("JAVA_CLASS_ON_COMPANION")
        @JvmStatic
        private val logger = LoggerFactory.getLogger(javaClass.enclosingClass)
    }

    private var prop: Properties = Properties()
    private var consumer: KafkaConsumer<String, String>
    private var polling = false

    init {
        prop[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = "127.0.0.1:9092"
        prop[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        prop[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        prop[ConsumerConfig.GROUP_ID_CONFIG] = groupId ?: "default-group" // elvis operator!!
        prop[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest" // latest or none
        consumer = KafkaConsumer(prop)
        consumer.subscribe(topics ?: mutableListOf("app_topic")) // mutable list creation + elvis operator!!

        logger.info("consumer registered. ${prop} to topic ${topics ?: mutableListOf("app_topic")}")
    }

    fun start() {
        polling = true
        while (polling) {
            val records = consumer.poll(Duration.of(1000L, ChronoUnit.MILLIS))
            for(i in records) {
                logger.info("Key ${i.key()}, Partition ${i.partition()}, Value ${i.value()}, Offset ${i.offset()}")
            }
        }
    }

    fun stop() {
        polling = false
    }

}

fun main(args: Array<String>) {

    val c = ConsumerDemo(mutableListOf("app_topic"), "my_first_app")

    c.start()

}
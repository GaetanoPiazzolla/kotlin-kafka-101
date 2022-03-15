package com.gae.piaz.kafka.kotlin

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import java.util.Properties

class ProducerDemo {

    private var prop: Properties = Properties()
    private var producer: KafkaProducer<String,String>

    init {
        prop[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = "127.0.0.1:9092"
        prop[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
        prop[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
        producer= KafkaProducer(prop)
    }

    fun send(message:String) {
        val record = ProducerRecord<String,String>("app_topic",message)
        this.producer.send(record)
        // flush data
        this.producer.flush()
        // flush and close
        this.producer.close()
    }


}

fun main(args: Array<String>) {

    val producer = ProducerDemo()
    producer.send("ciao")


}
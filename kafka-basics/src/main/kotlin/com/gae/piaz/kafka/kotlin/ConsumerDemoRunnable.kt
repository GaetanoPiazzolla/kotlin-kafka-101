package com.gae.piaz.kafka.kotlin

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.temporal.ChronoUnit
import java.util.*
import java.util.concurrent.CountDownLatch

class ConsumerDemoRunnable(
    private val topics: List<String>?,
    private val groupId: String?,
    private val countDownLatch: CountDownLatch
) : Runnable {

    companion object {
        @Suppress("JAVA_CLASS_ON_COMPANION")
        @JvmStatic
        private val logger = LoggerFactory.getLogger(javaClass.enclosingClass)
    }

    private var prop: Properties = Properties()
    private var consumer: KafkaConsumer<String, String>

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

    override fun run() {
        try {
            while (true) {
                val records = consumer.poll(Duration.of(1000L, ChronoUnit.MILLIS))
                for (i in records) {
                    logger.info("Key ${i.key()}, Partition ${i.partition()}, Value ${i.value()}, Offset ${i.offset()}")
                }
            }
        } catch (e: WakeupException) {
            logger.info("consumer has stopped, received shutdown signal.")
        } finally {
            consumer.close()
            countDownLatch.countDown()
        }
    }

    fun shutdown() {
        consumer.wakeup() // trow WakeUpException.
    }

}

fun main(args: Array<String>) {

    val latch = CountDownLatch(1)
    val consumerRunnable = ConsumerDemoRunnable(mutableListOf("app_topic"), "my_sixth_app", latch)

    val thread = Thread(consumerRunnable)
    thread.start()

    Runtime.getRuntime().addShutdownHook(Thread {
        consumerRunnable.shutdown();
        try {
            latch.await()
        }catch(e: InterruptedException) {
            e.printStackTrace()
        }
    })

}
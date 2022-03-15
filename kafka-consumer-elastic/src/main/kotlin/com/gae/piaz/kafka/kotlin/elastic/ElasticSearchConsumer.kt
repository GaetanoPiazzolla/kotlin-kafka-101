package com.gae.piaz.kafka.kotlin.elastic

import co.elastic.clients.elasticsearch.ElasticsearchClient
import co.elastic.clients.elasticsearch._types.Refresh
import co.elastic.clients.elasticsearch.core.IndexRequest
import co.elastic.clients.json.jackson.JacksonJsonpMapper
import co.elastic.clients.transport.ElasticsearchTransport
import co.elastic.clients.transport.rest_client.RestClientTransport
import org.apache.http.HttpHost
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.elasticsearch.client.RestClient
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.*

class ElasticSearchConsumer {

    companion object {
        @Suppress("JAVA_CLASS_ON_COMPANION")
        @JvmStatic
        private val logger = LoggerFactory.getLogger(javaClass.enclosingClass)
    }

    var client: ElasticsearchClient
    var consumer: KafkaConsumer<String, String>

    init {
        client = createClient()
        consumer = createConsumer()
        start()
    }

    private fun start() {
        while (true) {
            val records: ConsumerRecords<String, String> = consumer.poll(Duration.ofMillis(100))
            for (record in records) {

                val appData = AppData(record.key(), record.value())

                val docId = client.index { b: IndexRequest.Builder<Any?> ->
                    b.index("twitter").document(appData).refresh(Refresh.True)
                }.id()

                logger.info("inserted into elastic document with id $docId")
            }
        }
    }

    private fun createConsumer(): KafkaConsumer<String, String> {
        val prop = Properties()
        prop[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = "127.0.0.1:9092"
        prop[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        prop[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        prop[ConsumerConfig.GROUP_ID_CONFIG] = "kafka-demo-elasticsearch"
        prop[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"
        logger.info("consumer registered. $prop ")
        val con: KafkaConsumer<String, String> = KafkaConsumer(prop)
        con.subscribe(mutableListOf("twitter_tweets"))
        return con;
    }

    private fun createClient(): ElasticsearchClient {
        val hostname = "localhost"
        val builder = RestClient.builder(HttpHost(hostname, 9200, "http"))
        val restClient = builder.build()
        val transport: ElasticsearchTransport = RestClientTransport(
            restClient, JacksonJsonpMapper()
        )
        return ElasticsearchClient(transport)
    }

    fun disconnectClient() {
        logger.info("disconnecting CLIENT!!")
        client._transport().close()
    }

}

fun main(args: Array<String>) {
   val consumer = ElasticSearchConsumer()
    Runtime.getRuntime().addShutdownHook(Thread {
        consumer.disconnectClient();
    })
}
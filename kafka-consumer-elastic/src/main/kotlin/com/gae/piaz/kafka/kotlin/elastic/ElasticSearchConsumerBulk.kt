package com.gae.piaz.kafka.kotlin.elastic

import co.elastic.clients.elasticsearch.ElasticsearchClient
import co.elastic.clients.elasticsearch._types.Refresh
import co.elastic.clients.elasticsearch.core.BulkRequest
import co.elastic.clients.elasticsearch.core.IndexRequest
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation
import co.elastic.clients.elasticsearch.core.bulk.IndexOperation
import co.elastic.clients.json.jackson.JacksonJsonpMapper
import co.elastic.clients.transport.ElasticsearchTransport
import co.elastic.clients.transport.rest_client.RestClientTransport
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.http.HttpHost
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.elasticsearch.client.RestClient
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.*


class ElasticSearchConsumerBulk {

    companion object {
        @Suppress("JAVA_CLASS_ON_COMPANION")
        @JvmStatic
        private val logger = LoggerFactory.getLogger(javaClass.enclosingClass)
        private var mapper = ObjectMapper()
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

            logger.info("received ${records.count()} records")

            val operations = mutableListOf<BulkOperation>()

            for (record in records) {

                var id = extractIdFromTweet(record.value())
                if (id == null) {
                    id = "${record.topic()}_${record.partition()}_${record.offset()}"
                }

                val appData = AppData(record.key(), record.value())
                val operation = IndexOperation.Builder<AppData>().index("twitter").document(appData).id(id).build()
                val bulkOperation = BulkOperation.Builder().index(operation).build()

                operations.add(bulkOperation)

                logger.info("inserted into elastic document with id $id")

            }

            if(records.count() > 0) {
                val builder = BulkRequest.Builder()
                builder.operations(operations)
                client.bulk(builder.build())
                logger.info("committing the offset")
                consumer.commitSync()
                logger.info("committed")
                Thread.sleep(1000)
            }
        }
    }

    private fun extractIdFromTweet(value: String?): String? {
        val node: ObjectNode = mapper.readValue(value, ObjectNode::class.java)
        if (node.has("id_str")) {
            return node["id_str"].asText()
        }
        return null
    }

    private fun createConsumer(): KafkaConsumer<String, String> {
        val prop = Properties()
        prop[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = "127.0.0.1:9092"
        prop[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        prop[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        prop[ConsumerConfig.GROUP_ID_CONFIG] = "kafka-demo-elasticsearch"
        prop[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"

        prop[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG] = "false" // <- need commitSynch of offset!
        prop[ConsumerConfig.MAX_POLL_RECORDS_CONFIG] = "100" //<- 10 records per poll!

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
    val consumer = ElasticSearchConsumerBulk()
    Runtime.getRuntime().addShutdownHook(Thread {
        consumer.disconnectClient();
    })
}
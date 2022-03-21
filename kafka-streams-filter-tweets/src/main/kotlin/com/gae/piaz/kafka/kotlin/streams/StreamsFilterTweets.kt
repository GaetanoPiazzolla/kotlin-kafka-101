package com.gae.piaz.kafka.kotlin.streams

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import java.util.Properties

fun main() {

    val properties = Properties()

    properties[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = "127.0.0.1:9092"
    properties[StreamsConfig.APPLICATION_ID_CONFIG] ="demo-kafka-streams"
    properties[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.StringSerde::class.java::getName
    properties[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = Serdes.StringSerde::class.java::getName

    val builder = StreamsBuilder()

    val inputTopic = builder.stream<String,String>("twitter_topics")

    val filteredStream = inputTopic.filter{ k: String, jsonTweet:String ->
        extractFollowersFromTweet(jsonTweet).compareTo(10) > 10
    }
    filteredStream.to("important_tweets")

    val kafkaStreams = KafkaStreams(builder.build(),properties)
    kafkaStreams.start()
}

fun extractFollowersFromTweet(value: String?): Int {
    val mapper = ObjectMapper()
    val node: ObjectNode = mapper.readValue(value, ObjectNode::class.java)
    if (node.has("user")) {
        node["user"].get("followers_count").asInt()
    }
    return 1
}
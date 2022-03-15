package com.gae.piaz.kafka.kotlin.elastic

fun main() {

    val twitterClient = TwitterClient("bitcoin")

    val producer = TwitterProducer(twitterClient, true, true)

    Runtime.getRuntime().addShutdownHook(Thread {
        twitterClient.closeConnection()
        producer.close()
    })

}
package com.gae.piaz.kafka.kotlin.elastic

import com.google.common.collect.Lists
import com.twitter.hbc.ClientBuilder
import com.twitter.hbc.core.Client
import com.twitter.hbc.core.Constants
import com.twitter.hbc.core.Hosts
import com.twitter.hbc.core.HttpHosts
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint
import com.twitter.hbc.core.processor.StringDelimitedProcessor
import com.twitter.hbc.httpclient.auth.Authentication
import com.twitter.hbc.httpclient.auth.OAuth1
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit


class TwitterClient(val term:String) {
    companion object {
        @Suppress("JAVA_CLASS_ON_COMPANION")
        @JvmStatic
        private val logger = LoggerFactory.getLogger(javaClass.enclosingClass)
    }

    private val innerClient: Client
    private val innerQueue: BlockingQueue<String>

    init {

        TwitterClient::class.java.classLoader.getResourceAsStream("application.properties").use { input ->
            val prop = Properties()
            prop.load(input)

            val api_key = prop.getProperty("api_key")
            val api_key_secret = prop.getProperty("api_key_secret")
            val access_token= prop.getProperty("access_token")
            val access_token_secret= prop.getProperty("access_token_secret")
            val bearer_token = prop.getProperty("bearer_token")

            innerQueue = LinkedBlockingQueue(100000)
            val hosts: Hosts = HttpHosts(Constants.STREAM_HOST)
            val hosebirdEndpoint = StatusesFilterEndpoint()
            val terms: List<String> = Lists.newArrayList(term)
            hosebirdEndpoint.trackTerms(terms)
            val auth: Authentication = OAuth1(api_key, api_key_secret, access_token, access_token_secret)
            val builder: ClientBuilder = ClientBuilder()
                .name("twitter-kafka-client") // optional: mainly for the logs
                .hosts(hosts)
                .authentication(auth)
                .endpoint(hosebirdEndpoint)
                .processor(StringDelimitedProcessor(innerQueue))

            innerClient=builder.build()
            innerClient.connect()
            println("Initialized TwitterClient");
        }
    }

    fun closeConnection() {
        innerClient.stop();
    }

    fun isDone(): Boolean {
        return innerClient.isDone
    }

    fun takeMessage(): String? {
        val msg: String?
        try {
            msg = innerQueue.poll(5, TimeUnit.SECONDS)
            logger.info("just received message: $msg")
            return msg;
        }
        catch(e:Exception) {
            e.printStackTrace()
            innerClient.stop()
            return null
        }
    }

}

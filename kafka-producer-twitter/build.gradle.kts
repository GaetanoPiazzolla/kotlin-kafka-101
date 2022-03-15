plugins {
    kotlin("jvm")
    java
}

group = "com.gae.piaz.kafka.kotlin"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {

    implementation(kotlin("stdlib"))
    implementation("org.apache.kafka:kafka-clients:3.1.0")
    implementation("org.slf4j:slf4j-simple:1.7.36")

    implementation("com.twitter:hbc-core:2.2.0")


    testImplementation("org.junit.jupiter:junit-jupiter-api:5.8.2")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")

}

tasks.getByName<Test>("test") {
    useJUnitPlatform()
}
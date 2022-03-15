plugins {
    kotlin("jvm") version "1.6.10"
    java
}

group = "com.gae.piaz.kafka.kotlin"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

tasks.getByName<Test>("test") {
    useJUnitPlatform()
}
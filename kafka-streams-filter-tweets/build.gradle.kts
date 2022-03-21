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
    implementation("org.apache.kafka:kafka-streams:3.1.0")
    implementation("org.slf4j:slf4j-simple:1.7.36")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.12.3")

    implementation(kotlin("stdlib"))
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.6.0")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")
}

tasks.getByName<Test>("test") {
    useJUnitPlatform()
}
plugins {
    `java-library`
    id("io.spring.dependency-management") version "1.1.7"
    kotlin("jvm") version "2.0.21"
    kotlin("plugin.spring") version "2.0.21"
}

group = "dev.viren"
version = "0.1.0-SNAPSHOT"

java {
    toolchain { languageVersion = JavaLanguageVersion.of(17) }
    withSourcesJar()
}

repositories { mavenCentral() }

dependencyManagement {
    imports { mavenBom("org.springframework.boot:spring-boot-dependencies:3.5.7") }
}

dependencies {
    // Always required: the HTTP engine, JSON, Spring Boot auto-configuration.
    api("org.springframework.boot:spring-boot-autoconfigure")
    api("org.apache.httpcomponents.client5:httpclient5")
    api("com.fasterxml.jackson.core:jackson-databind")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310")
    implementation("org.jetbrains.kotlin:kotlin-reflect")
    implementation("org.slf4j:slf4j-api")

    // Optional features. The host application brings the ones it wants; each feature is
    // switched on by @ConditionalOnClass + a property, so a missing jar simply disables it.
    compileOnly("org.springframework.boot:spring-boot-starter-data-redis") // distributed rate limit, cooldown, replay guard
    compileOnly("org.springframework.kafka:spring-kafka")                  // async processing
    compileOnly("org.springframework:spring-jdbc")                         // transactional outbox
    compileOnly("org.springframework.boot:spring-boot-starter-web")        // inbound webhook endpoint
    compileOnly("org.springframework.boot:spring-boot-actuator")           // circuit breaker health
    compileOnly("io.micrometer:micrometer-core")                           // metrics

    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.springframework.boot:spring-boot-starter-web")
    testImplementation("org.springframework.boot:spring-boot-starter-jdbc")
    testImplementation("org.springframework.boot:spring-boot-starter-data-redis")
    testImplementation("org.springframework.boot:spring-boot-starter-actuator")
    testImplementation("org.springframework.kafka:spring-kafka")
    testImplementation("org.springframework.kafka:spring-kafka-test")
    testImplementation("io.micrometer:micrometer-core")
    testImplementation("org.awaitility:awaitility")
    testRuntimeOnly("com.h2database:h2")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

kotlin {
    compilerOptions { freeCompilerArgs.addAll("-Xjsr305=strict") }
}

tasks.withType<Test> {
    useJUnitPlatform()
    testLogging { events("failed"); exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL }
}

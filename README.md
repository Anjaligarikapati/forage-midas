# Midas Core

Completed project from the JPMorgan Chase & Co. Advanced Software Engineering Forage program.

## Overview

A Spring Boot application that processes financial transactions through a Kafka message queue, persists them to an H2 database, integrates with an external incentives API, and exposes a REST endpoint for balance queries.

## Stack

- Java 17
- Spring Boot 3.2.5
- Spring Data JPA + H2
- Spring Kafka
- Maven

## Tasks completed

1. Project setup with Spring Boot, JPA, Kafka, H2, and Testcontainers dependencies
2. Kafka listener that consumes transactions from the `trader-updates` topic
3. Transaction validation and persistence with H2 (sender/recipient balance updates, many-to-one relationships)
4. Integration with external incentives REST API; incentives added to recipient balance
5. REST controller exposing `GET /balance?userId=...` on port 33400

## Running locally

Start the incentives API in one terminal:

```
java -jar services/transaction-incentive-api.jar
```

Build and run Midas Core in another:

```
./mvnw spring-boot:run
```

## Running the tests

```
./mvnw -Dtest=TaskOneTests test
./mvnw -Dtest=TaskTwoTests test
./mvnw -Dtest=TaskThreeTests test
./mvnw -Dtest=TaskFourTests test
./mvnw -Dtest=TaskFiveTests test
```

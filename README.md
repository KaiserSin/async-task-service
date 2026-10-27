# Async Task Service

A Spring Boot service that receives tasks through Kafka and runs them asynchronously with a configurable worker pool. It stores task progress and results in PostgreSQL, exposes them through a REST API, and supports multiple instances sharing one database.

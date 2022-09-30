# Reactor Kafka - experiments

## Deferred Commits
- a number of separated commits that cannot be committed (not acknowledgements)
- the algorithm affected by `ConsumerConfig.MAX_POLL_RECORDS_CONFIG` , even if the `DEFERRED_COMMITS` reaches the threshold, 
the remaining records are processed. (debug - `org.apache.kafka.clients.consumer.KafkaConsumer.pollForFetches`)
# kafka-exception-iterator
Kafka exception management strategy that auto pause and iterate messages if message is processed in that iteration. 

### Produce without header

```shell
jq -rc . internal/testdata/exceptionMsg.json | kafka-console-producer --bootstrap-server 127.0.0.1:9092 --topic exception
```

### Produce with header

```shell
jq -rc . internal/testdata/exceptionMsg.json | kcat -b 127.0.0.1:9092 -t exception -P -H x-retry-count=0
```
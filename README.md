# kafka-exception-iterator
Kafka exception management strategy that auto pause and iterate messages if message is processed in that iteration. 


```shell
jq -rc . internal/testdata/exceptionMsg.json | kafka-console-producer --bootstrap-server 127.0.0.1:9092 --topic exception
```
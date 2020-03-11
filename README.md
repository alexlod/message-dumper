This simple utility will dump a Kafka message to a binary file.

Run from IntelliJ with the following arguments:

```
<bootstrap-server> <topic> <partition> <message-offset> <output-file>
```

For example:

```
localhost:9092 my-topic 2 100100 /tmp/binary-message
```

To connect to a secure Kafka cluster, you'll need to modify the code
to provide needed security configuration.
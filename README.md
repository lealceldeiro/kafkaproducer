# Example Java application: Apache Kafka Producer

An example of a producer Java application that sends events to an Apache Kafka broker

## Requirements

- Have installed Apache Kafka
  (see [How to run it sing Docker](https://lealceldeiro.github.io/gems/KafkaTheDefinitiveGuide/Chapter2/))
- Create a topic in the broker. i.e.: `topic1`

## Build and run

- Run `./mvnw compile exec:java -Dexec.mainClass="com.kafkaexamples.kafkaproducer.KafkaProducerMain" -Dexec.args="localhost:9094 topic1"`
- Start writing messages (strings) to be sent to the configured broker
- Type `/exit` and hit enter to stop the program

> Note: replace the provided arguments with the correct values for your Kafka cluster.

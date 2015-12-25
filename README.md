# SparkStreamingDemonstration

## Parts of Demostration:
####1. Stateless
####2. Stateful

## Stateless Demo

### Basic

#####1. QuickStreaming
This is a straightforward demostration of how to use spark streaming, it is borrowed from http://spark.apache.org/docs/latest/streaming-programming-guide.html

#####2. SparkSQLStreaming
This is a demo of how to use spark streaming with spark sql, more info at http://spark.apache.org/docs/latest/streaming-programming-guide.html#dataframe-and-sql-operations

#####3. WindowedQuickStreaming
This is a demo of how to use windowing feature of spark streaming, as we sample at 1 seconds, but have a sliding window of 30 seconds at interval of 10 seconds. More info at http://spark.apache.org/docs/latest/streaming-programming-guide.html#transformations-on-dstreams

#####4. JoiningQuickStreaming
This is a demo of how to join a normal RDD with DStream, the RDD is built from a file in the ./files folder. More info at http://spark.apache.org/docs/latest/streaming-programming-guide.html#transformations-on-dstreams

### Advanced

#####1. FlumeStreaming
This is a demo of how to draw data from Flume into Spark, using FlumeUtils. More info at http://spark.apache.org/docs/latest/streaming-flume-integration.html

#####2. RabbitMQStreaming
This is a demo of how to draw data from RabbitMQ into Spark, using RabbitMQUtils. More info at https://github.com/Stratio/RabbitMQ-Receiver

## Stateful Demo

### Basic

#####1. StatefulQuickStreaming
This is a demo of using updateStateByKey feature which can be used to maintain previous state and compute aggregation of current data with previous state. More info at http://spark.apache.org/docs/latest/streaming-programming-guide.html#transformations-on-dstreams

### Advanced

#####1. RabbitMQFaultTolerentStreaming
This is a demo of using RabbitMQ with checkpoints to save previous states, so that if a node goes down, previous computations are not lost. More info at http://spark.apache.org/docs/latest/streaming-programming-guide.html#checkpointing

#####2. StatefulRabbitMQFaultTolerentStreaming
This is a demo of using RabbitMQ with checkpoints to save previous states as well as previous computed state, giving both fault tolerance and aggregation of previous and current data. More info at http://spark.apache.org/docs/latest/streaming-programming-guide.html#checkpointing

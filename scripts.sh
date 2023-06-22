sudo yum install java-1.8*
wget https://downloads.apache.org/kafka/3.4.1/kafka_2.13-3.4.1.tgz
tar -xvf kafka_2.13-3.4.1.tgz
Start server
cd kafka_2.13-3.4.1

bin/zookeeper-server-start.sh config/zookeeper.properties

Start server:
bin/kafka-server-start.sh config/server.properties
Create the topic:
-----------------------------
Duplicate the session & enter in a new console --
cd kafka_2.12-3.3.1
bin/kafka-topics.sh --create --topic stockprices --bootstrap-server 52.54.239.175:9092 --replication-factor 1 --partitions 1

Start Producer:
--------------------------
bin/kafka-console-producer.sh --topic stockprices --bootstrap-server 52.54.239.175:9092

Start Consumer:
-------------------------
Duplicate the session & enter in a new console --
cd kafka_2.12-3.3.1
bin/kafka-console-consumer.sh --topic stockprices --bootstrap-server 52.54.239.175:9092
#
# VERSION               0.0.1
#
FROM      ubuntu

MAINTAINER Attila Balogh-Biró "attila.balogh86@gmail.com" 

# Installing basic stuf: maven, git, jdk

RUN apt-get update
RUN apt-get install -y openjdk-7-jdk
RUN apt-get install -y git
RUN apt-get install -y maven
RUN apt-get install -y wget
RUN apt-get install -y zookeeperd

# Set JAVA_HOME environment variable
ENV JAVA_HOME /usr/lib/jvm/java-7-openjdk-amd64

# Register automatic start for zookeeper
RUN /etc/init.d/zookeeper start

# Download Apache Kafka
RUN wget "http://mirror.cc.columbia.edu/pub/software/apache/kafka/0.8.2.2/kafka_2.11-0.8.2.2.tgz" -O /kafka.tgz

# Create folder for kafka installation
RUN mkdir -p /kafka

# Extract the archive
RUN tar -xvzf /kafka.tgz --strip 1 -C /kafka

# Start Kafka
RUN nohup /kafka/bin/kafka-server-start.sh /kafka/config/server.properties > /kafka/kafka.log 2>&1 &

# Create topic
RUN /kafka/bin/kafka-topics.sh --zookeeper localhost:2181 --create --replication-factor 1 --partition 1 --topic neverwinter

# Pull project from GitHub
RUN git clone https://github.com/PhaceBP/storm-flink-kafka-performance-test.git

# Pull forked Nvent project from GitHub
RUN git clone https://github.com/PhaceBP/interviewQuestions.git

# Step into the project root dir 
WORKDIR "/storm-flink-kafka-performance-test"

# Clean and build the project
RUN mvn clean install

# Finished

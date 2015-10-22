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

# Download Apache Kafka
RUN wget "http://mirror.cc.columbia.edu/pub/software/apache/kafka/0.8.2.2/kafka_2.11-0.8.2.2.tgz" -O /kafka.tgz

# Extract the archive
RUN tar -xvzf /kafka.tgz --strip 1

nohup /kafka/bin/kafka-server-start.sh /kafka/config/server.properties > /kafka/kafka.log 2>&1 &

# Pull project from GitHub
RUN git clone https://PhaceBP:admin123@github.com/PhaceBP/storm-flink-kafka-performance-test.git

# Step into the project root dir 
WORKDIR "/storm-flink-kafka-performance-test"

# Clean and build the project
RUN mvn clean install

# Finished

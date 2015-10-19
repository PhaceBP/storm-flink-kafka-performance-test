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

# Set JAVA_HOME environment variable
ENV JAVA_HOME /usr/lib/jvm/java-7-openjdk-amd64

# Pull project from GitHub
RUN git clone https://PhaceBP:admin123@github.com/PhaceBP/storm-flink-kafka-performance-test.git /workspace

# Step into the project root dir
WORKDIR "/workspace/storm-flink-kafka-performance-test"

# Print the current directory
RUN pwd

# Clean and build the project
RUN mvn clean install

# Finished

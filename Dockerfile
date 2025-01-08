#FROM ubuntu:22.04
#FROM maven:3.6.3-jdk-11
FROM openjdk:11-jdk-slim
# tomcat/jetty, mongo, kafka
EXPOSE 8080 27017 9092

WORKDIR /app

#COPY . /app/

#CMD ["/bin/bash"]
COPY target/springboot-kafka-spark-mongo-1.0-SNAPSHOT.jar /app/springboot-app.jar
ENTRYPOINT ["java","--add-exports", "java.base/sun.nio.ch=ALL-UNNAMED","-jar","/app/springboot-app.jar"]

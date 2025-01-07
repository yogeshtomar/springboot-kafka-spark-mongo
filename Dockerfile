FROM ubuntu:22.04
FROM maven:3.6.3-jdk-11
# tomcat/jetty, mongo, kafka
EXPOSE 8080 27017 9092

WORKDIR /app

COPY . /app/

CMD ["/bin/bash"]


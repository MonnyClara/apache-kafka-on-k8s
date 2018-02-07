FROM plugins/git as checkout_jetcd
WORKDIR /src
RUN git clone https://github.com/coreos/jetcd.git


FROM openjdk:8-jdk-stretch as builder
RUN apt-get update && apt-get install maven -y
WORKDIR  /jetcd
COPY --from=checkout_jetcd /src/jetcd .
RUN mvn install -DskipTests
ADD . /kafka
WORKDIR  /kafka
RUN ./gradlew -PscalaVersion=2.12 clean releaseTarGz


FROM openjdk:8-jdk-alpine
WORKDIR /opt/kafka
COPY --from=builder /kafka/core/build/distributions/kafka_2.12-1.2.0-SNAPSHOT.tgz /tmp
RUN tar -xzf /tmp/kafka_2.12-1.2.0-SNAPSHOT.tgz -C /opt/kafka --strip-components=1

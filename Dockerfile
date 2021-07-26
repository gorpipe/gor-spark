FROM nextcode/basespark:3.1.2

# To build nextcode/basespark:[version] base image on mac
# brew install apache-spark
# cd /usr/local/Cellar/apache-spark/[version]/libexec/
# docker build --build-arg java_image_tag=15-slim --build-arg spark_uid=3000 -t nextcode/basespark:[version] -f kubernetes/dockerfiles/spark/Dockerfile .
# docker push

COPY metrics.properties /etc/metrics/conf/metrics.properties
COPY prometheus.yaml /etc/metrics/conf/prometheus.yaml
COPY spark/build/install/spark/lib /opt/spark/jars

USER root
RUN rm -rf /opt/spark/jars/netty-all-4.0.23.Final.jar
RUN rm -rf /opt/spark/jars/jetty-6.1.26.jar
RUN rm -rf /opt/spark/jars/jetty-util-6.1.26.jar
RUN rm -rf /opt/spark/jars/jersey-client-1.19.jar
RUN rm -rf /opt/spark/jars/jersey-server-1.19.jar
RUN rm -rf /opt/spark/jars/log4j-over-slf4j-1.7.30.jar
RUN rm -rf /opt/spark/jars/logback-core-1.2.3.jar
RUN rm -rf /opt/spark/jars/logback-classic-1.2.3.jar
RUN rm -rf /opt/spark/jars/guava-14.0.1.jar

RUN apt install -y \
    apt-transport-https \
    ca-certificates \
    curl \
    gnupg \
    lsb-release

RUN curl -fsSL https://download.docker.com/linux/debian/gpg | gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg
RUN echo "deb [arch=amd64 signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/debian $(lsb_release -cs) stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null
RUN apt update
RUN apt install -y docker-ce-cli

USER 3000

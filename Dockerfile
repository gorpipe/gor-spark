FROM nextcode/ubuntuspark:3.1.2

# To build nextcode/basespark:[version] base image on mac
# brew install apache-spark
# cd /usr/local/Cellar/apache-spark/[version]/libexec/
# docker build --build-arg java_image_tag=15-slim --build-arg spark_uid=3000 -t nextcode/basespark:[version] -f kubernetes/dockerfiles/spark/Dockerfile .
# docker push

COPY spark/src/main/jib/etc/metrics/conf/metrics.properties /etc/metrics/conf/metrics.properties
COPY spark/src/main/jib/etc/metrics/conf/prometheus.yaml /etc/metrics/conf/prometheus.yaml
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

WORKDIR /opt/spark/work-dir

USER app

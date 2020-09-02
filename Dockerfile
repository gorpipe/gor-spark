ARG java_image_tag=14-slim
ARG spark_uid=3000

FROM openjdk:${java_image_tag}

RUN set -ex && \
    sed -i 's/http:\/\/deb.\(.*\)/https:\/\/deb.\1/g' /etc/apt/sources.list && \
    apt-get update && \
    ln -s /lib /lib64 && \
    apt install -y bash tini libc6 libpam-modules krb5-user libnss3 procps && \
    mkdir -p /opt/spark && \
    mkdir -p /opt/spark/examples && \
    mkdir -p /opt/spark/work-dir && \
    touch /opt/spark/RELEASE && \
    rm /bin/sh && \
    ln -sv /bin/bash /bin/sh && \
    echo "auth required pam_wheel.so use_uid" >> /etc/pam.d/su && \
    chgrp root /etc/passwd && chmod ug+rw /etc/passwd && \
    rm -rf /var/cache/apt/*

COPY spark/build/install/gor-scripts/lib /opt/spark/jars
COPY spark/src/main/docker/bin /opt/spark/bin
COPY spark/src/main/docker/sbin /opt/spark/sbin
COPY spark/src/main/docker/entrypoint.sh /opt/
COPY spark/src/main/docker/decom.sh /opt/

ENV SPARK_HOME /opt/spark

WORKDIR /opt/spark/work-dir
RUN chmod g+w /opt/spark/work-dir
RUN chmod a+x /opt/decom.sh

RUN rm -rf /opt/spark/jars/netty-all-4.0.23.Final.jar
RUN rm -rf /opt/spark/jars/jetty-6.1.26.jar
RUN rm -rf /opt/spark/jars/jetty-util-6.1.26.jar
RUN rm -rf /opt/spark/jars/jersey-client-1.19.jar
RUN rm -rf /opt/spark/jars/jersey-server-1.19.jar
RUN rm -rf /opt/spark/jars/log4j-over-slf4j-1.7.30.jar
RUN rm -rf /opt/spark/jars/kubernetes-client-4.5.2.jar
RUN rm -rf /opt/spark/jars/kubernetes-model-4.5.2.jar
RUN rm -rf /opt/spark/jars/kubernetes-model-common-4.5.2.jar
RUN rm -rf /opt/spark/jars/logback-core-1.2.3.jar
RUN rm -rf /opt/spark/jars/logback-classic-1.2.3.jar

RUN rm -rf /opt/spark/jars/hadoop-client-2.7.4.jar
RUN rm -rf /opt/spark/jars/hadoop-mapreduce-client-app-2.7.4.jar
RUN rm -rf /opt/spark/jars/hadoop-mapreduce-client-common-2.7.4.jar
RUN rm -rf /opt/spark/jars/hadoop-mapreduce-client-jobclient-2.7.4.jar
RUN rm -rf /opt/spark/jars/hadoop-mapreduce-client-shuffle-2.7.4.jar
RUN rm -rf /opt/spark/jars/hadoop-yarn-server-common-2.7.4.jar
RUN rm -rf /opt/spark/jars/hadoop-yarn-server-nodemanager-2.7.4.jar
RUN rm -rf /opt/spark/jars/hadoop-hdfs-2.7.4.jar

ENTRYPOINT [ "/opt/entrypoint.sh" ]

USER ${spark_uid}

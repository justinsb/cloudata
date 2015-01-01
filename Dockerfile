FROM ubuntu:14.04

RUN apt-get update && apt-get install --yes --no-install-recommends wget openssl ca-certificates

RUN cd /tmp \
    && wget -qO jdk8.tar.gz \
       --header "Cookie: oraclelicense=accept-securebackup-cookie" \
       http://download.oracle.com/otn-pub/java/jdk/8u25-b17/jdk-8u25-linux-x64.tar.gz \
    && tar xzf jdk8.tar.gz -C /opt \
    && mv /opt/jdk* /opt/java \
    && rm /tmp/jdk8.tar.gz \
    && update-alternatives --install /usr/bin/java java /opt/java/bin/java 100 \
    && update-alternatives --install /usr/bin/javac javac /opt/java/bin/javac 100

RUN apt-get install --yes --no-install-recommends maven
ADD . /src/
#RUN cd /src; mvn install

RUN cd /src; mvn install assembly:single

CMD cd /src/cloudata-git/target/cloudata-git-1.0-SNAPSHOT-bundle; /opt/java/bin/java -cp "lib/*" com.cloudata.git.GitServer
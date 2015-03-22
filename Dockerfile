FROM justinsb/java8

ADD . /src/
#RUN cd /src; mvn install

RUN cd /src; mvn install assembly:single

CMD cd /src/cloudata-git/target/cloudata-git-1.0-SNAPSHOT-bundle; /opt/java/bin/java -cp "lib/*" com.cloudata.git.GitServer

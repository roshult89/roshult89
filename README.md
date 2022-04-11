##Flink course home assignment

#comments
Since I only have a mac M1 available, I have some problems with my docker-images from time to time.
Got most of it to work with docker, but not flink itself.
Instead I followed https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/try-flink/local_installation/ to run it locally.
I also updated flink-conf.yml in ./conf so I had 3 taskmanagers, instead of one.

If runned by another in docker "bootstrap-servers" and "db-url" can be given as an attribute.

If not an arm processor is used, the images in docker-compose file should probably be updated for the correct processor,
i.e. by removing the platform flag

#Solution
I used my own solution from  https://github.com/apache/flink-training/tree/release-1.14/long-ride-alerts,
since it worked with all tests. I also think my solution handle the leaks mentioned in the discussion in the repo.

#Way to start
run
```
docker compose up -d
```

add images for flink to docker and restart docker or follow 
https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/try-flink/local_installation/
and update flink-conf.yml to have more than 1 taskmanager.

build the jars by
```
./gradlew clean spotlessApply build
```

open flink in localhost:8081 (or another port if specified)

upload jars from generator and detecter with name *snapshot-ALL.jar

verify entries by connecting to db by preferred tool (I used Intellij) and look into table taxi_alarm

db-url: jdbc:postgresql://localhost:5432/postgres
db-username: postgres
db-password

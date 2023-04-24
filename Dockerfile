FROM openjdk:19

COPY target/batchprocessing-0.0.1-SNAPSHOT.jar details-docker-batchprocessing.jar

ENTRYPOINT ["java","-jar","/details-docker-batchprocessing.jar"]
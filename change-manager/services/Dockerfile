FROM adoptopenjdk/openjdk11:alpine-jre
EXPOSE 80
ARG JAR_FILE=build/libs/services-1.0-SNAPSHOT.jar
ADD ${JAR_FILE} app.jar
ENTRYPOINT ["java","-jar","/app.jar"]
FROM maven:3.9.9-eclipse-temurin-17 AS build
WORKDIR /app
COPY pom.xml ./
COPY src ./src
RUN mvn -B -DskipTests package

FROM eclipse-temurin:17-jre-jammy
WORKDIR /app
COPY --from=build /app/target/weather-monitoring-1.0-SNAPSHOT-all.jar /app/app.jar
ENV MAIN_CLASS=CentralStation
ENTRYPOINT ["sh", "-c", "java -cp /app/app.jar ${MAIN_CLASS}"]
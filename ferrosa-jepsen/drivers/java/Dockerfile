FROM maven:3.9-eclipse-temurin-21 AS builder
WORKDIR /app
COPY pom.xml .
RUN mvn dependency:go-offline
COPY src/ src/
RUN mvn package -DskipTests

FROM eclipse-temurin:21-jre-alpine
COPY --from=builder /app/target/ferrosa-jepsen-0.8.8.8.jar /app/
COPY --from=builder /app/target/lib/ /app/lib/
ENTRYPOINT ["java", "-cp", "/app/ferrosa-jepsen-0.8.8.8.jar:/app/lib/*", "com.ferrosa.jepsen.FerrosJepsen"]

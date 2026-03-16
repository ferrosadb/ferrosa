FROM maven:3.9-eclipse-temurin-21 AS builder

WORKDIR /tests
COPY pom.xml ./
RUN mvn dependency:resolve -q

COPY CqlSmokeTest.java src/main/java/ferrosa/test/CqlSmokeTest.java
RUN mvn compile -q

FROM eclipse-temurin:21-jre

WORKDIR /tests
COPY --from=builder /tests/target/classes /tests/classes
COPY --from=builder /root/.m2/repository /root/.m2/repository
COPY --from=builder /tests/pom.xml /tests/pom.xml

# Copy dependencies to a flat lib directory for simpler classpath
COPY --from=builder /tests/target/dependency /tests/lib

CMD ["java", "-cp", "classes:lib/*", "ferrosa.test.CqlSmokeTest"]

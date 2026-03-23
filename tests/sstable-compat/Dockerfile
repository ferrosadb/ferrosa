# Builds Cassandra from source and generates SSTable test fixtures.
#
# Usage:
#   docker build -t ferrosa-sstable-gen -f tests/sstable-compat/Dockerfile .
#   docker run --rm -v ./ferrosa-sstable/tests/fixtures/cassandra_generated:/output ferrosa-sstable-gen

FROM eclipse-temurin:17-jdk AS builder

# Install Ant for Cassandra build
RUN apt-get update && apt-get install -y ant python3 && rm -rf /var/lib/apt/lists/*

WORKDIR /cassandra
COPY cassandra/ .

# Build Cassandra (needed for CQLSSTableWriter classes)
RUN ant build -q 2>/dev/null || ant build

# Compile our fixture generator
COPY tests/sstable-compat/CassandraSSTableWriter.java /gen/
RUN javac -cp "build/classes/main:lib/*" /gen/CassandraSSTableWriter.java -d /gen/

FROM eclipse-temurin:17-jre

COPY --from=builder /cassandra/build/classes/main /cassandra/classes
COPY --from=builder /cassandra/lib /cassandra/lib
COPY --from=builder /gen/*.class /gen/

CMD ["java", "-cp", "/gen:/cassandra/classes:/cassandra/lib/*", \
     "CassandraSSTableWriter", "/output"]

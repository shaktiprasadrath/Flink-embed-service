# syntax=docker/dockerfile:1

# ---- build stage: compile with the Gradle wrapper on JDK 21 -------------------------------------
FROM eclipse-temurin:21-jdk-noble AS build
WORKDIR /src

# Build files first, so the dependency download is cached until they change.
COPY gradlew settings.gradle build.gradle ./
COPY gradle gradle
# gradlew may have Windows line endings after a checkout with core.autocrlf=true
RUN sed -i 's/\r$//' gradlew && chmod +x gradlew
RUN --mount=type=cache,target=/root/.gradle ./gradlew --no-daemon dependencies --configuration runtimeClasspath -q

COPY src src
# Tests are run outside the image build (./gradlew test); they start ~25 Flink jobs.
RUN --mount=type=cache,target=/root/.gradle ./gradlew --no-daemon installDist -x test

# ---- runtime stage: JRE only -------------------------------------------------------------------
# glibc-based image (Ubuntu): RocksDB's native library (UC-16) does not work on Alpine/musl.
FROM eclipse-temurin:21-jre-noble
WORKDIR /app

# installDist layout: bin/flink-embed-service (start script with Flink's --add-opens flags) + lib/*.jar
COPY --from=build /src/build/install/flink-embed-service/ /app/
COPY data /app/data

# Run as an unprivileged user. Flink, RocksDB and UC-16 only write to /tmp.
RUN useradd --system --uid 10001 --home-dir /app flinkdemo
USER 10001

# Picked up by the start script; container-aware heap sizing + where the input files are.
ENV JAVA_OPTS="-XX:MaxRAMPercentage=75 -Dflinkdemo.data.dir=/app/data"

# 8081 = Flink Web UI (UC-21), 8082 = H2 console (when enabled)
EXPOSE 8081 8082

ENTRYPOINT ["/app/bin/flink-embed-service"]
CMD ["all"]

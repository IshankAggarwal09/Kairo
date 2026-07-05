# ── Stage 1: Build ────────────────────────────────────────────────────
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /app
COPY pom.xml .
# Download dependencies first (layer caching — only re-downloads if pom changes)
RUN mvn dependency:go-offline -q
COPY src ./src
RUN mvn clean package -q -DskipTests

# ── Stage 2: Run ──────────────────────────────────────────────────────
FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
COPY --from=build /app/target/kairo-1.0-SNAPSHOT.jar kairo.jar

# Port is configured at runtime via KAIRO_PORT env var (default 8080).
# EXPOSE is documentation — the actual port is controlled by the env var.
ENV KAIRO_PORT=8080
EXPOSE 8080

ENTRYPOINT ["java", "-jar", "kairo.jar"]

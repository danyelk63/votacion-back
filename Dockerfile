# ---- 1. Compilación ----
FROM maven:3.9-eclipse-temurin-21 AS compilacion
WORKDIR /src
COPY pom.xml .
RUN mvn -q -B dependency:go-offline
COPY src ./src
RUN mvn -q -B package -DskipTests

# ---- 2. Ejecución: JRE + nginx ----
FROM eclipse-temurin:21-jre-jammy
RUN apt-get update \
 && apt-get install -y --no-install-recommends nginx \
 && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=compilacion /src/target/votacion.jar /app/votacion.jar
COPY docker/nginx.conf.plantilla /app/nginx.conf.plantilla
COPY docker/arranque.sh /app/arranque.sh
RUN chmod +x /app/arranque.sh
ENV PORT=10000
EXPOSE 10000
CMD ["/app/arranque.sh"]

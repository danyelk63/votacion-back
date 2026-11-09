#!/usr/bin/env bash
set -euo pipefail

PUERTO="${PORT:-10000}"
JAR=/app/votacion.jar
JVM="${JVM_OPTS:--Xms24m -Xmx64m -Xss256k -XX:+UseSerialGC -XX:MaxMetaspaceSize=48m -XX:ReservedCodeCacheSize=16m -XX:TieredStopAtLevel=1 -XX:CICompilerCount=1 -Djava.rmi.server.hostname=127.0.0.1}"

N1=1=127.0.0.1:1101; N2=2=127.0.0.1:1102; N3=3=127.0.0.1:1103

java $JVM -jar $JAR nodo 1 1101 "$N2,$N3" &
java $JVM -jar $JAR nodo 2 1102 "$N1,$N3" &
java $JVM -jar $JAR nodo 3 1103 "$N1,$N2" &
java $JVM -jar $JAR gateway 8080 "$N1,$N2,$N3" 8081 1100 &

sed "s/__PUERTO__/$PUERTO/" /app/nginx.conf.plantilla > /tmp/nginx.conf
nginx -c /tmp/nginx.conf -g 'daemon off;' &

echo "[arranque] sistema iniciado; puerto público $PUERTO"
wait -n
echo "[arranque] un proceso terminó; deteniendo el contenedor"
exit 1

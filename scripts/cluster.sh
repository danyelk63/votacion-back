#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

JAR=target/votacion.jar
[ -f "$JAR" ] || mvn -q -B package -DskipTests

N1=1=localhost:1101; N2=2=localhost:1102; N3=3=localhost:1103

java -jar "$JAR" nodo 1 1101 "$N2,$N3" & P1=$!
java -jar "$JAR" nodo 2 1102 "$N1,$N3" & P2=$!
java -jar "$JAR" nodo 3 1103 "$N1,$N2" & P3=$!
java -jar "$JAR" gateway 8080 "$N1,$N2,$N3" & PG=$!

echo "PIDs -> nodo1=$P1 nodo2=$P2 nodo3=$P3 gateway=$PG"
echo "Para tumbar un nodo de verdad: kill <PID>   (o usa los botones del frontend)"
trap 'kill $P1 $P2 $P3 $PG 2>/dev/null' INT TERM EXIT
wait

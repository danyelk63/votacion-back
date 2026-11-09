@echo off
REM Levanta 3 nodos + gateway en ventanas separadas (Windows).
cd /d "%~dp0\.."
if not exist target\votacion.jar call mvn -q -B package -DskipTests

start "Nodo 1" java -jar target\votacion.jar nodo 1 1101 "2=localhost:1102,3=localhost:1103"
start "Nodo 2" java -jar target\votacion.jar nodo 2 1102 "1=localhost:1101,3=localhost:1103"
start "Nodo 3" java -jar target\votacion.jar nodo 3 1103 "1=localhost:1101,2=localhost:1102"
start "Gateway" java -jar target\votacion.jar gateway 8080 "1=localhost:1101,2=localhost:1102,3=localhost:1103"

echo Cluster iniciado. Para tumbar un nodo cierre su ventana o use los botones del frontend.

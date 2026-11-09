# votacion-back

Sistema **distribuido** de registro de votos con **Java RMI**. Ejercicio académico sin base de datos: los votos viven en memoria, **replicados** en varios nodos que se ponen de acuerdo con el algoritmo de consenso **Raft**.

Cada voto registra el **nombre**, la **cédula de ciudadanía** y uno de **2 candidatos**. Una cédula solo puede votar una vez en todo el clúster.

## Arquitectura

```
 Navegador (Angular · votacion-front)
        │  HTTP / JSON  (el navegador no puede hablar RMI)
        ▼
 ┌──────────────┐
 │   Gateway    │  cliente RMI · balanceo round-robin · failover
 └──────┬───────┘
        │  Java RMI  (cliente → servidor)
        ▼
 ┌─────────┐   RMI   ┌─────────┐   RMI   ┌─────────┐
 │ Nodo 1  │◄───────►│ Nodo 2  │◄───────►│ Nodo 3  │   (servidor ↔ servidor)
 │ Registry│         │ Registry│         │ Registry│
 │  :1101  │◄───────────────────────────►│  :1103  │
 └─────────┘         └─────────┘         └─────────┘
   Cada nodo es un proceso JVM independiente con su propio RMI Registry,
   donde publica su objeto remoto "NodoVotacion".
```

RMI se usa en **dos niveles**:

| Nivel | Quién llama | Métodos remotos | Para qué |
|---|---|---|---|
| Cliente → servidor | Gateway → nodo | `registrarVoto`, `resultados`, `votos`, `estado`, `pausar`, `reanudar` | Atender al usuario |
| Servidor ↔ servidor | Nodo → nodo | `solicitarVoto`, `anexarEntradas` | Elegir líder y replicar los votos (Raft) |
| Servidor ↔ servidor | Seguidor → líder | `registrarVoto`, `resultados` | Reenviar escrituras y lecturas al líder |

### Piezas RMI en el código

| Concepto RMI | Archivo |
|---|---|
| Interfaz remota (`extends Remote`, métodos `throws RemoteException`) | [`comun/NodoVotacion.java`](src/main/java/co/votacion/comun/NodoVotacion.java) |
| Objeto remoto (`extends UnicastRemoteObject`) | [`nodo/ServicioNodo.java`](src/main/java/co/votacion/nodo/ServicioNodo.java) |
| Creación del Registry y `rebind` | [`nodo/ServidorNodo.java`](src/main/java/co/votacion/nodo/ServidorNodo.java) |
| `LocateRegistry.getRegistry(...).lookup(...)` (obtener el stub) | [`comun/ClienteRmi.java`](src/main/java/co/votacion/comun/ClienteRmi.java) |
| Objetos que viajan por la red (`Serializable`) | `comun/*.java` (records) |
| Excepción de negocio que viaja por RMI | [`comun/VotoRechazadoException.java`](src/main/java/co/votacion/comun/VotoRechazadoException.java) |
| Timeouts de RMI para detectar caídas | `ClienteRmi.configurarTimeouts` |

## ¿Por qué es un sistema distribuido?

- **Replicación**: cada voto queda copiado en todos los nodos. Se confirma cuando está en la **mayoría** (2 de 3).
- **Tolerancia a fallos**: si cae un nodo, incluso el líder, el sistema sigue funcionando. Con 3 nodos tolera 1 caído; con 5, tolera 2.
- **Elección de líder**: si el líder deja de enviar latidos, los demás eligen uno nuevo de forma automática.
- **Consistencia**: todos los nodos aplican los mismos votos en el **mismo orden**. Una cédula no puede votar dos veces aunque las peticiones lleguen por nodos distintos al mismo tiempo.
- **Transparencia**: el cliente puede hablar con cualquier nodo. Si no es el líder, este reenvía la petición al líder por RMI. El gateway además hace failover si un nodo no responde.
- **Recuperación**: un nodo que se cae y vuelve (aunque llegue vacío) recibe del líder todo lo que le faltaba.

### Raft en 5 puntos

1. Cada nodo es **seguidor**, **candidato** o **líder**. El tiempo se divide en **términos** numerados.
2. El líder envía **latidos** (`anexarEntradas` vacío) cada 300 ms. Si un seguidor no recibe latidos en 1,5–3 s (tiempo aleatorio), se vuelve candidato, sube el término y pide votos (`solicitarVoto`). Gana quien obtenga la mayoría.
3. Un voto ciudadano se convierte en una **entrada del log**. El líder la agrega a su log y la replica con `anexarEntradas`.
4. Cuando la mayoría tiene la entrada, el líder la **confirma** (commit). Todos la aplican a su "registro electoral" en memoria (`RegistroVotos`).
5. Solo puede ganar una elección un nodo con el log **al menos tan actualizado** como la mayoría. Así un voto confirmado nunca se pierde.

La implementación está en [`nodo/NodoRaft.java`](src/main/java/co/votacion/nodo/NodoRaft.java), comentada paso a paso.

## Requisitos

- Java 21+
- Maven 3.9+

## Ejecutar

```bash
mvn package                 # compila, corre las pruebas y genera target/votacion.jar
./scripts/cluster.sh        # Linux / macOS: 3 nodos + gateway
scripts\cluster.bat         # Windows: abre una ventana por proceso
```

O a mano, cada uno en su terminal:

```bash
java -jar target/votacion.jar nodo 1 1101 "2=localhost:1102,3=localhost:1103"
java -jar target/votacion.jar nodo 2 1102 "1=localhost:1101,3=localhost:1103"
java -jar target/votacion.jar nodo 3 1103 "1=localhost:1101,2=localhost:1102"
java -jar target/votacion.jar gateway 8080 "1=localhost:1101,2=localhost:1102,3=localhost:1103"
```

### En varias computadoras (misma red)

Usen la IP de cada máquina en lugar de `localhost` y arranquen cada nodo indicando su propia IP a RMI:

```bash
# en la máquina 192.168.1.10
java -Djava.rmi.server.hostname=192.168.1.10 -jar votacion.jar nodo 1 1101 "2=192.168.1.11:1102,3=192.168.1.12:1103"
```

Hay que abrir en el firewall los puertos 1101–1103 y el 8080.

## API REST del gateway (puerto 8080)

| Método | Ruta | Descripción |
|---|---|---|
| GET | `/api/candidatos` | Lista de candidatos |
| POST | `/api/votos` | Registrar voto: `{"nombre":"Ana Pérez","cedula":"1012345678","candidato":"C1"}` |
| GET | `/api/votos` | Votos registrados (cédula enmascarada) |
| GET | `/api/resultados` | Conteo y porcentaje por candidato |
| GET | `/api/cluster` | Estado de cada nodo: rol, término, líder, log, réplicas |
| POST | `/api/cluster/{id}/pausar` | Simula la caída de un nodo |
| POST | `/api/cluster/{id}/reanudar` | Lo recupera |

Códigos de respuesta de `POST /api/votos`:

- `200`: voto registrado y replicado.
- `400`: datos inválidos.
- `409`: la cédula ya votó.
- `503`: el clúster no tiene mayoría.

## Guion sugerido para la demo

1. Levantar el clúster y mostrar en los logs cómo un nodo **gana la elección**.
2. Registrar un voto y ver en las 3 consolas `aplica voto #N`.
3. Intentar votar con la misma cédula: rechazado (409).
4. **Tumbar al líder**: cerrar su ventana, hacer `kill`, o pausarlo desde el frontend. Ver cómo los otros eligen un nuevo líder en el siguiente término.
5. Registrar otro voto: funciona con 2 de 3 nodos.
6. Tumbar un segundo nodo e intentar votar: el sistema **se niega** (sin mayoría no hay consenso).
7. Levantar los nodos de nuevo: se ponen al día solos y todos muestran el mismo conteo.

## Pruebas

```bash
mvn test
```

[`ClusterTest`](src/test/java/co/votacion/ClusterTest.java) levanta 3 nodos y el gateway con RMI real y verifica:

- elección de un único líder
- replicación del voto en todos los nodos
- rechazo de cédulas duplicadas y de datos inválidos
- nueva elección tras la caída del líder y recuperación del nodo caído
- rechazo de votos cuando no hay mayoría
- mismo orden de votos en todos los nodos ante peticiones concurrentes

## Limitaciones (a propósito, por ser académico)

- Sin base de datos ni disco: si se apagan **todos** los nodos, se pierden los votos. Si se cae solo una minoría, no se pierde nada.
- Membresía fija del clúster (no se agregan nodos en caliente) y sin snapshots del log.
- El gateway es un único proceso. Se pueden levantar varios gateways apuntando a los mismos nodos, porque no guardan estado.

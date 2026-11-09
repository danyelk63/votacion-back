package co.votacion;

import co.votacion.comun.ClienteRmi;
import co.votacion.comun.DireccionNodo;
import co.votacion.gateway.Gateway;
import co.votacion.nodo.ConfigRaft;
import co.votacion.nodo.ServidorNodo;

import java.nio.file.Path;

/**
 * Punto de entrada único del jar:
 * <pre>
 *   java -jar votacion.jar nodo &lt;id&gt; &lt;puerto&gt; &lt;pares&gt;
 *       ej: java -jar votacion.jar nodo 1 1101 "2=localhost:1102,3=localhost:1103"
 *   java -jar votacion.jar gateway &lt;puertoHttp&gt; &lt;nodos&gt;
 *       ej: java -jar votacion.jar gateway 8080 "1=localhost:1101,2=localhost:1102,3=localhost:1103"
 *       opcional: [puertoWebSocket (8081)] [puertoCallbackRmi (1100)]
 * </pre>
 * Para correr nodos en máquinas distintas, use la IP real en los pares y arranque
 * cada nodo con {@code -Djava.rmi.server.hostname=<su IP>}.
 */
public class Main {

    /** Carpeta raíz de los archivos de texto (votos y logs). Se cambia con -Ddatos=... */
    private static final String DIR_DATOS = System.getProperty("datos", "datos");

    public static void main(String[] args) throws Exception {
        // Detectar rápido nodos caídos (por defecto RMI puede esperar mucho).
        ClienteRmi.configurarTimeouts(500, 8000);

        if (args.length >= 3 && args[0].equals("nodo")) {
            int id = Integer.parseInt(args[1]);
            int puerto = Integer.parseInt(args[2]);
            String pares = args.length > 3 ? args[3] : "";
            new ServidorNodo(id, puerto, DireccionNodo.parsear(pares), ConfigRaft.porDefecto(),
                    Path.of(DIR_DATOS, "nodo-" + id), false);
            Thread.currentThread().join(); // el registry RMI mantiene vivo el proceso
        } else if (args.length >= 3 && args[0].equals("gateway")) {
            int puerto = Integer.parseInt(args[1]);
            int puertoWs = args.length > 3 ? Integer.parseInt(args[3]) : puerto + 1;
            int puertoCallback = args.length > 4 ? Integer.parseInt(args[4]) : 1100;
            new Gateway(puerto, puertoWs, puertoCallback, DireccionNodo.parsear(args[2]),
                    Path.of(DIR_DATOS, "gateway"), false).iniciar();
        } else {
            System.err.println("""
                    Uso:
                      java -jar votacion.jar nodo <id> <puerto> "<id>=<host>:<puerto>,..."
                      java -jar votacion.jar gateway <puertoHttp> "<id>=<host>:<puerto>,..." [puertoWs] [puertoCallback]
                    """);
            System.exit(1);
        }
    }
}

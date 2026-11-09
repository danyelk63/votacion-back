package co.votacion;

import co.votacion.comun.ClienteRmi;
import co.votacion.comun.DireccionNodo;
import co.votacion.gateway.Gateway;
import co.votacion.nodo.ConfigRaft;
import co.votacion.nodo.ServidorNodo;

/**
 * Punto de entrada único del jar:
 * <pre>
 *   java -jar votacion.jar nodo &lt;id&gt; &lt;puerto&gt; &lt;pares&gt;
 *       ej: java -jar votacion.jar nodo 1 1101 "2=localhost:1102,3=localhost:1103"
 *   java -jar votacion.jar gateway &lt;puertoHttp&gt; &lt;nodos&gt;
 *       ej: java -jar votacion.jar gateway 8080 "1=localhost:1101,2=localhost:1102,3=localhost:1103"
 * </pre>
 * Para correr nodos en máquinas distintas, use la IP real en los pares y arranque
 * cada nodo con {@code -Djava.rmi.server.hostname=<su IP>}.
 */
public class Main {

    public static void main(String[] args) throws Exception {
        // Detectar rápido nodos caídos (por defecto RMI puede esperar mucho).
        ClienteRmi.configurarTimeouts(500, 8000);

        if (args.length >= 3 && args[0].equals("nodo")) {
            int id = Integer.parseInt(args[1]);
            int puerto = Integer.parseInt(args[2]);
            String pares = args.length > 3 ? args[3] : "";
            new ServidorNodo(id, puerto, DireccionNodo.parsear(pares), ConfigRaft.porDefecto(), false);
            Thread.currentThread().join(); // el registry RMI mantiene vivo el proceso
        } else if (args.length >= 3 && args[0].equals("gateway")) {
            int puerto = Integer.parseInt(args[1]);
            new Gateway(puerto, DireccionNodo.parsear(args[2]), false).iniciar();
        } else {
            System.err.println("""
                    Uso:
                      java -jar votacion.jar nodo <id> <puerto> "<id>=<host>:<puerto>,..."
                      java -jar votacion.jar gateway <puertoHttp> "<id>=<host>:<puerto>,..."
                    """);
            System.exit(1);
        }
    }
}

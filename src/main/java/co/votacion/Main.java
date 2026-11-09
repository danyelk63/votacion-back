package co.votacion;

import co.votacion.comun.ClienteRmi;
import co.votacion.comun.DireccionNodo;
import co.votacion.gateway.Gateway;
import co.votacion.nodo.ConfigRaft;
import co.votacion.nodo.ServidorNodo;

import java.nio.file.Path;

public class Main {
    private static final String DIR_DATOS = System.getProperty("datos", "datos");

    public static void main(String[] args) throws Exception {
        ClienteRmi.configurarTimeouts(500, 8000);

        if (args.length >= 3 && args[0].equals("nodo")) {
            int id = Integer.parseInt(args[1]);
            int puerto = Integer.parseInt(args[2]);
            String pares = args.length > 3 ? args[3] : "";
            new ServidorNodo(id, puerto, DireccionNodo.parsear(pares), ConfigRaft.porDefecto(),
                    Path.of(DIR_DATOS, "nodo-" + id), false);
            Thread.currentThread().join();
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

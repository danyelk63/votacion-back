package co.votacion.comun;

import java.util.Arrays;
import java.util.List;

/** Dirección de un nodo: host y puerto de su RMI Registry. */
public record DireccionNodo(int id, String host, int puerto) {

    /** Convierte "1=localhost:1101,2=localhost:1102" en una lista de direcciones. */
    public static List<DireccionNodo> parsear(String texto) {
        if (texto == null || texto.isBlank()) return List.of();
        return Arrays.stream(texto.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .map(par -> {
                    String[] idYDir = par.split("=");
                    String[] hostYPuerto = idYDir[1].split(":");
                    return new DireccionNodo(Integer.parseInt(idYDir[0].trim()), hostYPuerto[0].trim(),
                            Integer.parseInt(hostYPuerto[1].trim()));
                })
                .toList();
    }

    @Override
    public String toString() {
        return "rmi://" + host + ":" + puerto + "/" + NodoVotacion.NOMBRE_RMI;
    }
}

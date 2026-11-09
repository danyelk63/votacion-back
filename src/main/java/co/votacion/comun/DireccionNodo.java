package co.votacion.comun;

import java.util.Arrays;
import java.util.List;

public record DireccionNodo(int id, String host, int puerto) {
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

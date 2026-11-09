package co.votacion.comun;

import java.io.Serializable;
import java.util.Map;

public record EstadoNodo(int id, String rol, long termino, Integer liderId, Integer votoPara, boolean pausado,
                         int longitudLog, int indiceConfirmado, int ultimoAplicado, int totalVotos,
                         Map<Integer, Integer> replicacion) implements Serializable {
}

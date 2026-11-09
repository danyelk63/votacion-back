package co.votacion.comun;

import java.io.Serializable;
import java.util.Map;

/**
 * @param replicacion solo en el líder: id de cada seguidor -> último índice replicado en él
 */
public record EstadoNodo(int id, String rol, long termino, Integer liderId, Integer votoPara, boolean pausado,
                         int longitudLog, int indiceConfirmado, int ultimoAplicado, int totalVotos,
                         Map<Integer, Integer> replicacion) implements Serializable {
}

package co.votacion.comun;

import java.io.Serializable;
import java.util.List;

public record SolicitudAnexar(long termino, int liderId, int indiceLogPrevio, long terminoLogPrevio,
                              List<EntradaLog> entradas, int confirmadoLider) implements Serializable {
}

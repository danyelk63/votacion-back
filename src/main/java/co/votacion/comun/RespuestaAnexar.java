package co.votacion.comun;

import java.io.Serializable;

public record RespuestaAnexar(long termino, boolean exito, int indiceCoincidente, int indiceConflicto)
        implements Serializable {
}

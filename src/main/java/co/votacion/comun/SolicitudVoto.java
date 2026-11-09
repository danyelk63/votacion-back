package co.votacion.comun;

import java.io.Serializable;

public record SolicitudVoto(long termino, int candidatoId, int ultimoIndiceLog, long ultimoTerminoLog)
        implements Serializable {
}

package co.votacion.comun;

import java.io.Serializable;

/**
 * Petición para borrar todos los votos.
 *
 * @param reenviadoPor nodo seguidor que la reenvió al líder (null si viene del gateway)
 * @param traza        id de trazabilidad asignado por el gateway
 */
public record SolicitudBorrado(Integer reenviadoPor, String traza) implements Serializable {

    public SolicitudBorrado reenviadaPor(int nodoId) {
        return new SolicitudBorrado(nodoId, traza);
    }
}

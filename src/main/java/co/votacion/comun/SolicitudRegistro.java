package co.votacion.comun;

import java.io.Serializable;

/**
 * Petición de registro de un voto.
 *
 * @param reenviadoPor id del nodo seguidor que reenvió la petición al líder (null si viene del gateway)
 * @param traza        id de trazabilidad asignado por el gateway al recibir la petición HTTP
 */
public record SolicitudRegistro(String nombre, String cedula, String candidato, Integer reenviadoPor, String traza)
        implements Serializable {

    public SolicitudRegistro reenviadaPor(int nodoId) {
        return new SolicitudRegistro(nombre, cedula, candidato, nodoId, traza);
    }
}

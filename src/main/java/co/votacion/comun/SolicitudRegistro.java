package co.votacion.comun;

import java.io.Serializable;

/**
 * Petición de registro de un voto.
 *
 * @param reenviadoPor id del nodo seguidor que reenvió la petición al líder (null si viene del gateway)
 */
public record SolicitudRegistro(String nombre, String cedula, String candidato, Integer reenviadoPor)
        implements Serializable {

    public SolicitudRegistro reenviadaPor(int nodoId) {
        return new SolicitudRegistro(nombre, cedula, candidato, nodoId);
    }
}

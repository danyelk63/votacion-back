package co.votacion.comun;

import java.io.Serializable;

public record SolicitudRegistro(String nombre, String cedula, String candidato, Integer reenviadoPor, String traza)
        implements Serializable {
    public SolicitudRegistro reenviadaPor(int nodoId) {
        return new SolicitudRegistro(nombre, cedula, candidato, nodoId, traza);
    }
}

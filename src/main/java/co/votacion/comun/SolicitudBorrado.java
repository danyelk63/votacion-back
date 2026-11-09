package co.votacion.comun;

import java.io.Serializable;

public record SolicitudBorrado(Integer reenviadoPor, String traza) implements Serializable {
    public SolicitudBorrado reenviadaPor(int nodoId) {
        return new SolicitudBorrado(nodoId, traza);
    }
}

package co.votacion.comun;

import java.io.Serializable;

public record ResultadoBorrado(String mensaje, int votosBorrados, int indiceLog, long termino, int lider,
                               Integer reenviadoPor) implements Serializable {
}

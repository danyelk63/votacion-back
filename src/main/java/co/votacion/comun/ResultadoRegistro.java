package co.votacion.comun;

import java.io.Serializable;

public record ResultadoRegistro(String mensaje, int indiceLog, long termino, int lider, String registradoEn,
                                Integer reenviadoPor) implements Serializable {
}

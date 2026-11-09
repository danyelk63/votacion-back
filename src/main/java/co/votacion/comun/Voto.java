package co.votacion.comun;

import java.io.Serializable;

public record Voto(String nombre, String cedula, String candidato, String registradoEn, int recibidoPorNodo,
                   String traza)
        implements Serializable {
}

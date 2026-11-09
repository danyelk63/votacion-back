package co.votacion.comun;

import java.io.Serializable;

/** Voto tal como se guarda en el log replicado. */
public record Voto(String nombre, String cedula, String candidato, String registradoEn, int recibidoPorNodo,
                   String traza)
        implements Serializable {
}

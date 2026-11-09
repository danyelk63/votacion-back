package co.votacion.comun;

import java.io.Serializable;

/**
 * @param indiceCoincidente si exito: último índice que coincide con el líder
 * @param indiceConflicto   si no exito: desde dónde debe reintentar el líder
 */
public record RespuestaAnexar(long termino, boolean exito, int indiceCoincidente, int indiceConflicto)
        implements Serializable {
}

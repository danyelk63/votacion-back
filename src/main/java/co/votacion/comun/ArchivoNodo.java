package co.votacion.comun;

import java.io.Serializable;
import java.util.List;

/** Contenido del archivo de texto donde un nodo guarda los votos confirmados. */
public record ArchivoNodo(int nodoId, String ruta, int totalLineas, List<String> lineas) implements Serializable {
}

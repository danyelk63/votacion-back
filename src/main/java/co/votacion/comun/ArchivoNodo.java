package co.votacion.comun;

import java.io.Serializable;
import java.util.List;

public record ArchivoNodo(int nodoId, String ruta, int totalLineas, List<String> lineas) implements Serializable {
}

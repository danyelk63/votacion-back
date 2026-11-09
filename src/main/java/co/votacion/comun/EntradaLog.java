package co.votacion.comun;

import java.io.Serializable;

/**
 * Entrada del log replicado de Raft. {@code voto} es null para entradas de
 * control (centinela en el índice 0 y "noop" que agrega cada líder nuevo).
 */
public record EntradaLog(int indice, long termino, String id, Tipo tipo, Voto voto) implements Serializable {

    public enum Tipo { CENTINELA, NOOP, VOTO }

    public static EntradaLog centinela() {
        return new EntradaLog(0, 0, "centinela", Tipo.CENTINELA, null);
    }
}

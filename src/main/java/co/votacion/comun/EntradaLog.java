package co.votacion.comun;

import java.io.Serializable;

/**
 * Entrada del log replicado de Raft.
 *
 * <ul>
 *   <li>{@code VOTO}: un voto ciudadano ({@code voto} != null).</li>
 *   <li>{@code REINICIO}: orden de borrar todos los votos (se replica como cualquier otra
 *       entrada, así todos los nodos borran lo mismo en el mismo punto del log).</li>
 *   <li>{@code NOOP}: la agrega cada líder nuevo; {@code CENTINELA}: índice 0.</li>
 * </ul>
 *
 * @param traza id de trazabilidad de la petición que originó la entrada (null en NOOP/CENTINELA)
 */
public record EntradaLog(int indice, long termino, String id, Tipo tipo, Voto voto, String traza)
        implements Serializable {

    public enum Tipo { CENTINELA, NOOP, VOTO, REINICIO }

    public static EntradaLog centinela() {
        return new EntradaLog(0, 0, "centinela", Tipo.CENTINELA, null, null);
    }

    /** Entradas que corresponden a una petición de usuario y por eso se trazan. */
    public boolean rastreable() {
        return tipo == Tipo.VOTO || tipo == Tipo.REINICIO;
    }

    public String descripcion() {
        return tipo == Tipo.REINICIO ? "orden de BORRADO #" + indice : "entrada #" + indice;
    }
}

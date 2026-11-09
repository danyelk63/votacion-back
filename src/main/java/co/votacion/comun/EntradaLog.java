package co.votacion.comun;

import java.io.Serializable;

public record EntradaLog(int indice, long termino, String id, Tipo tipo, Voto voto, String traza)
        implements Serializable {
    public enum Tipo { CENTINELA, NOOP, VOTO, REINICIO }

    public static EntradaLog centinela() {
        return new EntradaLog(0, 0, "centinela", Tipo.CENTINELA, null, null);
    }

    public boolean rastreable() {
        return tipo == Tipo.VOTO || tipo == Tipo.REINICIO;
    }

    public String descripcion() {
        return tipo == Tipo.REINICIO ? "orden de BORRADO #" + indice : "entrada #" + indice;
    }
}

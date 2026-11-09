package co.votacion.comun;

import java.io.Serializable;

public record RespuestaVoto(long termino, boolean votoConcedido) implements Serializable {
}

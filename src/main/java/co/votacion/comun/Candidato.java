package co.votacion.comun;

import java.io.Serializable;
import java.util.List;

public record Candidato(String id, String nombre, String partido) implements Serializable {

    public static final List<Candidato> TODOS = List.of(
            new Candidato("C1", "Laura Gómez", "Movimiento Azul"),
            new Candidato("C2", "Andrés Ruiz", "Alianza Verde Clara"));

    public static boolean existe(String id) {
        return TODOS.stream().anyMatch(c -> c.id().equals(id));
    }
}

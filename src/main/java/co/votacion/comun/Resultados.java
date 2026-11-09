package co.votacion.comun;

import java.io.Serializable;
import java.util.List;

public record Resultados(int total, List<ResultadoCandidato> candidatos, int servidoPorNodo, boolean consistente)
        implements Serializable {
    public record ResultadoCandidato(String id, String nombre, String partido, int votos, double porcentaje)
            implements Serializable {
    }
}

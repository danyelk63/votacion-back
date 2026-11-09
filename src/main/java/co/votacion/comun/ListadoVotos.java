package co.votacion.comun;

import java.io.Serializable;
import java.util.List;

public record ListadoVotos(List<VotoPublico> votos, int servidoPorNodo, boolean consistente) implements Serializable {
    public record VotoPublico(String nombre, String cedula, String candidato, String registradoEn,
                              int recibidoPorNodo, int indiceLog) implements Serializable {
    }
}

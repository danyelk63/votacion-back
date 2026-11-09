package co.votacion.nodo;

import co.votacion.comun.Candidato;
import co.votacion.comun.EntradaLog;
import co.votacion.comun.ListadoVotos.VotoPublico;
import co.votacion.comun.Resultados.ResultadoCandidato;
import co.votacion.comun.Voto;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class RegistroVotos {
    record Registro(Voto voto, int indiceLog) {
    }

    private final Map<String, Registro> porCedula = new HashMap<>();
    private final List<Registro> enOrden = new ArrayList<>();
    private final Map<String, Integer> conteo = new LinkedHashMap<>();

    RegistroVotos() {
        Candidato.TODOS.forEach(c -> conteo.put(c.id(), 0));
    }

    void reiniciar() {
        porCedula.clear();
        enOrden.clear();
        conteo.replaceAll((k, v) -> 0);
    }

    boolean yaVoto(String cedula) {
        return porCedula.containsKey(cedula);
    }

    public int total() {
        return enOrden.size();
    }

    boolean aplicar(EntradaLog entrada) {
        if (entrada.tipo() != EntradaLog.Tipo.VOTO) return true;
        Voto voto = entrada.voto();
        if (porCedula.containsKey(voto.cedula())) return false;
        Registro r = new Registro(voto, entrada.indice());
        porCedula.put(voto.cedula(), r);
        enOrden.add(r);
        conteo.merge(voto.candidato(), 1, Integer::sum);
        return true;
    }

    public Map<String, Integer> conteo() {
        return new LinkedHashMap<>(conteo);
    }

    List<ResultadoCandidato> resultados() {
        int total = total();
        return Candidato.TODOS.stream().map(c -> {
            int votos = conteo.get(c.id());
            double porcentaje = total == 0 ? 0 : Math.round(votos * 10000.0 / total) / 100.0;
            return new ResultadoCandidato(c.id(), c.nombre(), c.partido(), votos, porcentaje);
        }).toList();
    }

    List<VotoPublico> listado() {
        return enOrden.stream().map(r -> new VotoPublico(r.voto().nombre(), enmascarar(r.voto().cedula()),
                r.voto().candidato(), r.voto().registradoEn(), r.voto().recibidoPorNodo(), r.indiceLog())).toList();
    }

    public List<String> cedulasEnOrden() {
        return enOrden.stream().map(r -> r.voto().cedula()).toList();
    }

    private static String enmascarar(String cedula) {
        return "*".repeat(cedula.length() - 4) + cedula.substring(cedula.length() - 4);
    }
}

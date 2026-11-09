package co.votacion.nodo;

import co.votacion.comun.Candidato;
import co.votacion.comun.SolicitudRegistro;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

final class Validacion {

    private static final Pattern NOMBRE = Pattern.compile("^[A-Za-zÁÉÍÓÚÜÑáéíóúüñ' ]{3,100}$");
    private static final Pattern CEDULA = Pattern.compile("^\\d{6,10}$");

    private Validacion() {
    }

    record Resultado(List<String> errores, String nombre, String cedula, String candidato) {
    }

    /** Valida y normaliza (quita espacios extra y puntos de la cédula). */
    static Resultado validar(SolicitudRegistro s) {
        String nombre = s.nombre() == null ? "" : s.nombre().trim().replaceAll("\\s+", " ");
        String cedula = s.cedula() == null ? "" : s.cedula().replaceAll("[.\\s]", "");
        String candidato = s.candidato() == null ? "" : s.candidato();

        List<String> errores = new ArrayList<>();
        if (!NOMBRE.matcher(nombre).matches()) errores.add("El nombre debe tener entre 3 y 100 letras");
        if (!CEDULA.matcher(cedula).matches()) errores.add("La cédula debe tener entre 6 y 10 dígitos");
        if (!Candidato.existe(candidato)) errores.add("Debe escoger uno de los candidatos");
        return new Resultado(errores, nombre, cedula, candidato);
    }
}

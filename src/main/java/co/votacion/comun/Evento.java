package co.votacion.comun;

import java.io.Serializable;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

public record Evento(long secuencia, String hora, String fuente, Integer nodoId, String rol, Long termino,
                     String categoria, String etapa, String traza, String mensaje, Map<String, String> datos)
        implements Serializable {
    public static Evento nuevo(String fuente, Integer nodoId, String rol, Long termino, String categoria,
                               String etapa, String traza, String mensaje, Map<String, String> datos) {
        return new Evento(0, Instant.now().toString(), fuente, nodoId, rol, termino, categoria, etapa, traza,
                mensaje, datos == null ? Map.of() : new LinkedHashMap<>(datos));
    }

    public Evento conSecuencia(long s) {
        return new Evento(s, hora, fuente, nodoId, rol, termino, categoria, etapa, traza, mensaje, datos);
    }

    public String comoLinea() {
        String quien = nodoId == null ? rol : "nodo " + nodoId + " " + rol + (termino == null ? "" : " t" + termino);
        return hora + " [" + quien + "] " + categoria + (etapa == null ? "" : "/" + etapa)
                + (traza == null ? "" : " traza=" + traza) + " :: " + mensaje
                + (datos.isEmpty() ? "" : " " + datos);
    }

    public static Map<String, String> datos(Object... kv) {
        Map<String, String> m = new LinkedHashMap<>();
        for (int i = 0; i + 1 < kv.length; i += 2) m.put(String.valueOf(kv[i]), String.valueOf(kv[i + 1]));
        return m;
    }

    public static String enmascarar(String cedula) {
        if (cedula == null || cedula.length() <= 4) return cedula;
        return "*".repeat(cedula.length() - 4) + cedula.substring(cedula.length() - 4);
    }
}

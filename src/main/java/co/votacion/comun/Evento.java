package co.votacion.comun;

import java.io.Serializable;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Evento de trazabilidad. Lo generan los nodos y el gateway; los nodos se lo
 * envían al gateway por <b>callback RMI</b> ({@link OyenteEventos}) y el gateway
 * lo reenvía a los navegadores por <b>WebSocket</b>.
 *
 * @param secuencia número global asignado por el gateway al recibirlo (0 si aún no se asignó)
 * @param hora      instante ISO-8601 con milisegundos en que ocurrió
 * @param fuente    "NODO" o "GATEWAY"
 * @param nodoId    id del nodo que lo generó (null para el gateway)
 * @param rol       tipo de nodo al momento del evento: LIDER, SEGUIDOR, CANDIDATO o GATEWAY
 * @param termino   término de Raft del nodo (null para el gateway)
 * @param categoria SISTEMA, ELECCION, VOTO, REPLICACION, PERSISTENCIA, RED o USUARIO
 * @param etapa     paso concreto del procesamiento (ej. ANEXADO_LOG, GUARDADO_ARCHIVO)
 * @param traza     id que sigue a un mismo voto por todo el sistema (null si no aplica)
 * @param mensaje   descripción legible
 * @param datos     detalles adicionales clave/valor
 */
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

    /** Línea de texto para consola y archivos de log. */
    public String comoLinea() {
        String quien = nodoId == null ? rol : "nodo " + nodoId + " " + rol + (termino == null ? "" : " t" + termino);
        return hora + " [" + quien + "] " + categoria + (etapa == null ? "" : "/" + etapa)
                + (traza == null ? "" : " traza=" + traza) + " :: " + mensaje
                + (datos.isEmpty() ? "" : " " + datos);
    }

    /** Ayuda para construir mapas de datos: datos("clave", valor, "clave2", valor2...). */
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

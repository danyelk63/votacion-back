package co.votacion.gateway;

import co.votacion.comun.Evento;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.List;

/**
 * Log de auditoría de usuarios: registra cada acción que hace una persona
 * desde el navegador (quién, cuándo, desde dónde, qué pidió y qué respondió el
 * sistema) en el archivo {@code datos/gateway/usuarios.log}, y además la publica
 * como evento USUARIO para verla en tiempo real.
 */
public class BitacoraUsuarios {

    static final String CABECERA = "# fecha | accion | resultado | nombre | cedula | candidato | ip | navegador | nodo_atendio | traza | detalle";

    private final Path archivo;
    private final CentralEventos central;

    public BitacoraUsuarios(Path archivo, CentralEventos central) throws IOException {
        this.archivo = archivo;
        this.central = central;
        Files.createDirectories(archivo.getParent());
        Files.writeString(archivo, "# Auditoría de usuarios (iniciado " + Instant.now() + ")" + System.lineSeparator()
                + CABECERA + System.lineSeparator(), StandardCharsets.UTF_8);
    }

    public Path archivo() {
        return archivo;
    }

    public record Accion(String accion, String resultado, String nombre, String cedula, String candidato, String ip,
                         String navegador, Integer nodo, String traza, String detalle) {
    }

    public synchronized void registrar(Accion a) {
        String cedula = Evento.enmascarar(a.cedula());
        String linea = String.join(" | ", Instant.now().toString(), a.accion(), a.resultado(), s(a.nombre()), s(cedula),
                s(a.candidato()), s(a.ip()), s(a.navegador()), s(a.nodo()), s(a.traza()), s(a.detalle()));
        try {
            Files.writeString(archivo, linea + System.lineSeparator(), StandardCharsets.UTF_8, StandardOpenOption.APPEND);
        } catch (IOException ignorada) {
        }
        central.gateway("USUARIO", a.accion(), a.traza(), descripcion(a), Evento.datos(
                "resultado", a.resultado(), "nombre", s(a.nombre()), "cedula", s(cedula), "candidato", s(a.candidato()),
                "ip", s(a.ip()), "navegador", s(a.navegador()), "nodoAtendio", s(a.nodo()), "detalle", s(a.detalle()),
                "archivo", archivo.toAbsolutePath()));
    }

    private static String descripcion(Accion a) {
        return switch (a.accion()) {
            case "VOTAR" -> "El usuario " + s(a.nombre()) + " (CC " + Evento.enmascarar(a.cedula()) + ") intentó votar: " + a.resultado();
            case "BORRAR_VOTOS" -> "El usuario ordenó BORRAR TODOS LOS VOTOS: " + a.resultado();
            case "TUMBAR_NODO", "REANUDAR_NODO" -> "El operador ejecutó " + a.accion() + " sobre el nodo " + s(a.nodo()) + ": " + a.resultado();
            default -> a.accion() + ": " + a.resultado();
        };
    }

    public synchronized List<String> leer(int max) {
        try {
            List<String> todas = Files.readAllLines(archivo, StandardCharsets.UTF_8);
            return List.copyOf(todas.subList(Math.max(0, todas.size() - max), todas.size()));
        } catch (IOException e) {
            return List.of();
        }
    }

    public synchronized int totalLineas() {
        try (var l = Files.lines(archivo, StandardCharsets.UTF_8)) {
            return (int) l.count();
        } catch (IOException e) {
            return 0;
        }
    }

    private static String s(Object o) {
        return o == null ? "-" : o.toString().replace("|", "/");
    }
}

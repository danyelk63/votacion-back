package co.votacion.gateway;

import co.votacion.comun.Evento;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

/**
 * Concentra todos los eventos del sistema (los que llegan de los nodos por
 * callback RMI y los que genera el propio gateway): les asigna un número de
 * secuencia global, guarda un historial y los reparte a los suscriptores
 * (el servidor WebSocket).
 */
public class CentralEventos {

    private static final int MAX_HISTORIAL = 5000;

    private final Deque<Evento> historial = new ArrayDeque<>();
    private final List<Consumer<Evento>> suscriptores = new CopyOnWriteArrayList<>();
    private final Path archivo;
    private final boolean consola;
    private long secuencia = 0;

    public CentralEventos(Path archivo, boolean consola) throws IOException {
        this.archivo = archivo;
        this.consola = consola;
        Files.createDirectories(archivo.getParent());
        Files.writeString(archivo, "", StandardCharsets.UTF_8);
    }

    public synchronized Evento publicar(Evento e) {
        Evento numerado = e.conSecuencia(++secuencia);
        historial.addLast(numerado);
        if (historial.size() > MAX_HISTORIAL) historial.removeFirst();
        if ("GATEWAY".equals(e.fuente())) {
            String linea = numerado.comoLinea();
            if (consola) System.out.println(java.text.Normalizer.normalize(linea, java.text.Normalizer.Form.NFD)
                    .replaceAll("\\p{M}", ""));
            try {
                Files.writeString(archivo, linea + System.lineSeparator(), StandardCharsets.UTF_8,
                        StandardOpenOption.APPEND);
            } catch (IOException ignorada) {
            }
        }
        for (Consumer<Evento> s : suscriptores) s.accept(numerado);
        return numerado;
    }

    /** Evento generado por el gateway. */
    public Evento gateway(String categoria, String etapa, String traza, String mensaje, java.util.Map<String, String> datos) {
        return publicar(Evento.nuevo("GATEWAY", null, "GATEWAY", null, categoria, etapa, traza, mensaje, datos));
    }

    public synchronized List<Evento> historial() {
        return new ArrayList<>(historial);
    }

    /** Suscribe y devuelve el historial de forma atómica (sin perder ni duplicar eventos). */
    public synchronized List<Evento> suscribir(Consumer<Evento> s) {
        suscriptores.add(s);
        return new ArrayList<>(historial);
    }

    public void desuscribir(Consumer<Evento> s) {
        suscriptores.remove(s);
    }
}

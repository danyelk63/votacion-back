package co.votacion.nodo;

import co.votacion.comun.Evento;
import co.votacion.comun.OyenteEventos;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Bitácora del nodo. Cada evento:
 * <ol>
 *   <li>se imprime en la consola del proceso,</li>
 *   <li>se agrega al archivo {@code eventos.log} del nodo,</li>
 *   <li>se envía por <b>callback RMI</b> a los oyentes suscritos (el gateway).</li>
 * </ol>
 * El envío remoto se hace en un hilo aparte (en orden) para no frenar el algoritmo.
 */
class Bitacora {

    private final Path archivo;
    private final boolean consola;
    private final CopyOnWriteArrayList<OyenteEventos> oyentes = new CopyOnWriteArrayList<>();
    private final ExecutorService envios = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "bitacora-envios");
        t.setDaemon(true);
        return t;
    });

    Bitacora(Path archivo, boolean consola) {
        this.archivo = archivo;
        this.consola = consola;
        try {
            Files.createDirectories(archivo.getParent());
            Files.writeString(archivo, "", StandardCharsets.UTF_8); // empieza vacío en cada arranque
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    Path archivo() {
        return archivo;
    }

    /** @return true si es un oyente nuevo. */
    boolean suscribir(OyenteEventos oyente) {
        return oyentes.addIfAbsent(oyente);
    }

    synchronized void publicar(Evento e) {
        String linea = e.comoLinea();
        // En consola sin tildes: muchas terminales (p. ej. la de Windows) no muestran bien UTF-8.
        if (consola) System.out.println(sinTildes(linea));
        try {
            Files.writeString(archivo, linea + System.lineSeparator(), StandardCharsets.UTF_8,
                    StandardOpenOption.APPEND);
        } catch (IOException ignorada) {
            // el log en archivo es auxiliar: no debe tumbar el nodo
        }
        if (!oyentes.isEmpty()) envios.submit(() -> entregar(e));
    }

    private void entregar(Evento e) {
        for (OyenteEventos o : oyentes) {
            try {
                o.recibir(e); // llamada RMI: nodo -> gateway
            } catch (Exception ex) {
                oyentes.remove(o); // el gateway se desconectó; volverá a suscribirse
            }
        }
    }

    private static String sinTildes(String s) {
        return java.text.Normalizer.normalize(s, java.text.Normalizer.Form.NFD).replaceAll("\\p{M}", "");
    }

    void detener() {
        envios.shutdownNow();
    }
}

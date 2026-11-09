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
            Files.writeString(archivo, "", StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    synchronized void vaciarArchivo() {
        try {
            Files.writeString(archivo, "", StandardCharsets.UTF_8);
        } catch (IOException ignorada) {
        }
    }

    Path archivo() {
        return archivo;
    }

    boolean suscribir(OyenteEventos oyente) {
        return oyentes.addIfAbsent(oyente);
    }

    synchronized void publicar(Evento e) {
        String linea = e.comoLinea();

        if (consola) System.out.println(sinTildes(linea));
        try {
            Files.writeString(archivo, linea + System.lineSeparator(), StandardCharsets.UTF_8,
                    StandardOpenOption.APPEND);
        } catch (IOException ignorada) {
        }
        if (!oyentes.isEmpty()) envios.submit(() -> entregar(e));
    }

    private void entregar(Evento e) {
        for (OyenteEventos o : oyentes) {
            try {
                o.recibir(e);
            } catch (Exception ex) {
                oyentes.remove(o);
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

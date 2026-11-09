package co.votacion.nodo;

import co.votacion.comun.ArchivoNodo;
import co.votacion.comun.EntradaLog;
import co.votacion.comun.Voto;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.List;

class AlmacenVotos {
    static final String CABECERA = "# fecha_guardado | indice_log | termino | cedula | nombre | candidato | nodo_lider | traza";

    private final int nodoId;
    private final Path archivo;
    private int lineas = 0;

    AlmacenVotos(int nodoId, Path archivo) {
        this.nodoId = nodoId;
        this.archivo = archivo;
        try {
            Files.createDirectories(archivo.getParent());
            Files.writeString(archivo, "# Votos confirmados del nodo " + nodoId + " (iniciado " + Instant.now() + ")"
                    + System.lineSeparator() + CABECERA + System.lineSeparator(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    Path archivo() {
        return archivo;
    }

    synchronized void reiniciarDesdeCero() throws IOException {
        Files.writeString(archivo, "# Votos confirmados del nodo " + nodoId + " (reiniciado " + Instant.now() + ")"
                + System.lineSeparator() + CABECERA + System.lineSeparator(), StandardCharsets.UTF_8);
        lineas = 0;
    }

    synchronized void reiniciar(EntradaLog orden) throws IOException {
        Files.writeString(archivo, "# Votos confirmados del nodo " + nodoId + System.lineSeparator()
                + "# Votos borrados el " + Instant.now() + " por la orden #" + orden.indice() + " (término "
                + orden.termino() + ", traza " + orden.traza() + ")" + System.lineSeparator()
                + CABECERA + System.lineSeparator(), StandardCharsets.UTF_8);
        lineas = 0;
    }

    synchronized int guardar(EntradaLog entrada) throws IOException {
        Voto v = entrada.voto();
        String linea = String.join(" | ", Instant.now().toString(), String.valueOf(entrada.indice()),
                String.valueOf(entrada.termino()), v.cedula(), v.nombre(), v.candidato(),
                String.valueOf(v.recibidoPorNodo()), String.valueOf(v.traza()));
        Files.writeString(archivo, linea + System.lineSeparator(), StandardCharsets.UTF_8, StandardOpenOption.APPEND);
        return ++lineas;
    }

    synchronized ArchivoNodo leer(int maxLineas) {
        try {
            List<String> todas = Files.readAllLines(archivo, StandardCharsets.UTF_8);
            List<String> ultimas = todas.subList(Math.max(0, todas.size() - maxLineas), todas.size());
            return new ArchivoNodo(nodoId, archivo.toAbsolutePath().toString(), todas.size(), List.copyOf(ultimas));
        } catch (IOException e) {
            return new ArchivoNodo(nodoId, archivo.toAbsolutePath().toString(), 0, List.of("(error leyendo: " + e.getMessage() + ")"));
        }
    }
}

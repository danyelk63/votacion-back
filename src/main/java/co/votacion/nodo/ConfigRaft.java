package co.votacion.nodo;

/**
 * Tiempos del algoritmo. El timeout de elección debe ser bastante mayor que el
 * intervalo de latido para que un líder sano no sea reemplazado.
 */
public record ConfigRaft(long latidoMs, long eleccionMinMs, long eleccionMaxMs, long timeoutConfirmacionMs) {

    public static ConfigRaft porDefecto() {
        return new ConfigRaft(300, 1500, 3000, 5000);
    }
}

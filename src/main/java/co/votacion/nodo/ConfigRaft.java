package co.votacion.nodo;

public record ConfigRaft(long latidoMs, long eleccionMinMs, long eleccionMaxMs, long timeoutConfirmacionMs) {
    public static ConfigRaft porDefecto() {
        return new ConfigRaft(300, 1500, 3000, 5000);
    }
}

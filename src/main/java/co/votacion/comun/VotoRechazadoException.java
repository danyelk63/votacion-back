package co.votacion.comun;

import java.util.List;

public class VotoRechazadoException extends Exception {
    public enum Codigo {
        DATOS_INVALIDOS(false),

        CEDULA_DUPLICADA(false),

        SIN_LIDER(true),

        NO_CONFIRMADO(true),

        TIMEOUT(false);

        public final boolean reintentable;

        Codigo(boolean reintentable) {
            this.reintentable = reintentable;
        }
    }

    private final Codigo codigo;
    private final List<String> detalles;

    public VotoRechazadoException(Codigo codigo, String mensaje) {
        this(codigo, mensaje, List.of());
    }

    public VotoRechazadoException(Codigo codigo, String mensaje, List<String> detalles) {
        super(mensaje);
        this.codigo = codigo;
        this.detalles = List.copyOf(detalles);
    }

    public Codigo getCodigo() {
        return codigo;
    }

    public List<String> getDetalles() {
        return detalles;
    }
}

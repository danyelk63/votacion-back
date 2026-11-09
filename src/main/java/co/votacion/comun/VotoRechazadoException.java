package co.votacion.comun;

import java.util.List;

/**
 * Excepción de negocio que viaja por RMI hasta el cliente (las excepciones
 * declaradas en una interfaz remota se serializan como cualquier objeto).
 */
public class VotoRechazadoException extends Exception {

    public enum Codigo {
        /** Datos del formulario inválidos. */
        DATOS_INVALIDOS(false),
        /** La cédula ya votó. */
        CEDULA_DUPLICADA(false),
        /** No hay líder elegido (elección en curso) o no se pudo contactar: otro nodo puede intentarlo. */
        SIN_LIDER(true),
        /** El voto se descartó porque cambió el líder antes de confirmarse: se puede reintentar. */
        NO_CONFIRMADO(true),
        /** No se logró mayoría a tiempo: el resultado es incierto. */
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

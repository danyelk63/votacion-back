package co.votacion.comun;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;

/**
 * Referencia perezosa al stub RMI de un nodo remoto. Hace el lookup en el
 * Registry la primera vez y, si una llamada falla, lo descarta para volver a
 * buscarlo (el nodo pudo reiniciarse y publicar un objeto remoto nuevo).
 */
public class ClienteRmi {

    @FunctionalInterface
    public interface Llamada<T> {
        T ejecutar(NodoVotacion nodo) throws Exception;
    }

    private final DireccionNodo direccion;
    private volatile NodoVotacion stub;

    public ClienteRmi(DireccionNodo direccion) {
        this.direccion = direccion;
    }

    public DireccionNodo direccion() {
        return direccion;
    }

    public int id() {
        return direccion.id();
    }

    public NodoVotacion stub() throws RemoteException {
        NodoVotacion s = stub;
        if (s == null) {
            try {
                s = (NodoVotacion) LocateRegistry.getRegistry(direccion.host(), direccion.puerto())
                        .lookup(NodoVotacion.NOMBRE_RMI);
            } catch (NotBoundException e) {
                throw new RemoteException("Nodo " + direccion.id() + " aún no publicado", e);
            }
            stub = s;
        }
        return s;
    }

    /** Ejecuta una llamada remota; ante un error de comunicación descarta el stub. */
    public <T> T llamar(Llamada<T> llamada) throws Exception {
        try {
            return llamada.ejecutar(stub());
        } catch (RemoteException e) {
            if (!(e instanceof NodoNoDisponibleException)) stub = null;
            throw e;
        }
    }

    /** Ajusta los timeouts de RMI para detectar rápido nodos caídos. Llamar antes de usar RMI. */
    public static void configurarTimeouts(int conexionMs, int respuestaMs) {
        System.setProperty("sun.rmi.transport.connectionTimeout", String.valueOf(conexionMs));
        System.setProperty("sun.rmi.transport.tcp.handshakeTimeout", String.valueOf(conexionMs));
        System.setProperty("sun.rmi.transport.tcp.responseTimeout", String.valueOf(respuestaMs));
        System.setProperty("sun.rmi.transport.proxy.connectTimeout", String.valueOf(conexionMs));
    }
}

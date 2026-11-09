package co.votacion.comun;

import java.rmi.RemoteException;

/** La lanza un nodo "pausado" para simular que está caído. */
public class NodoNoDisponibleException extends RemoteException {
    public NodoNoDisponibleException(int nodoId) {
        super("Nodo " + nodoId + " fuera de servicio (simulado)");
    }
}

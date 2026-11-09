package co.votacion.comun;

import java.rmi.RemoteException;

public class NodoNoDisponibleException extends RemoteException {
    public NodoNoDisponibleException(int nodoId) {
        super("Nodo " + nodoId + " fuera de servicio (simulado)");
    }
}

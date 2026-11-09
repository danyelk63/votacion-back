package co.votacion.comun;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface OyenteEventos extends Remote {
    void recibir(Evento evento) throws RemoteException;
}

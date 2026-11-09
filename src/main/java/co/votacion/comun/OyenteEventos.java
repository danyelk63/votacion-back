package co.votacion.comun;

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * Interfaz remota de <b>callback</b> RMI. El gateway exporta un objeto que la
 * implementa y se lo entrega a cada nodo con {@link NodoVotacion#suscribir}.
 * Así se invierte el sentido habitual: es el SERVIDOR (nodo) quien invoca un
 * método remoto en el CLIENTE (gateway) cada vez que ocurre algo.
 */
public interface OyenteEventos extends Remote {

    void recibir(Evento evento) throws RemoteException;
}

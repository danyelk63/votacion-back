package co.votacion.gateway;

import co.votacion.comun.Evento;
import co.votacion.comun.OyenteEventos;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

/**
 * Objeto remoto de <b>callback</b> que exporta el gateway. Se le entrega a cada
 * nodo con {@code suscribir(...)}; desde entonces el NODO invoca
 * {@link #recibir} en el gateway cada vez que genera un evento.
 */
public class ReceptorEventos extends UnicastRemoteObject implements OyenteEventos {

    private final transient CentralEventos central;

    public ReceptorEventos(CentralEventos central, int puerto) throws RemoteException {
        super(puerto);
        this.central = central;
    }

    @Override
    public void recibir(Evento evento) {
        central.publicar(evento);
    }
}

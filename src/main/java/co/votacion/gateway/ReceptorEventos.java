package co.votacion.gateway;

import co.votacion.comun.Evento;
import co.votacion.comun.OyenteEventos;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

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

package co.votacion.comun;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface NodoVotacion extends Remote {
    String NOMBRE_RMI = "NodoVotacion";

    RespuestaVoto solicitarVoto(SolicitudVoto solicitud) throws RemoteException;

    RespuestaAnexar anexarEntradas(SolicitudAnexar solicitud) throws RemoteException;

    ResultadoRegistro registrarVoto(SolicitudRegistro solicitud) throws RemoteException, VotoRechazadoException;

    ResultadoBorrado borrarVotos(SolicitudBorrado solicitud) throws RemoteException, VotoRechazadoException;

    Resultados resultados(boolean soloLocal) throws RemoteException;

    ListadoVotos votos(boolean soloLocal) throws RemoteException;

    EstadoNodo estado() throws RemoteException;

    void suscribir(OyenteEventos oyente) throws RemoteException;

    ArchivoNodo leerArchivo() throws RemoteException;

    EstadoNodo pausar() throws RemoteException;

    EstadoNodo reanudar() throws RemoteException;

    EstadoNodo reiniciarTodo() throws RemoteException;
}

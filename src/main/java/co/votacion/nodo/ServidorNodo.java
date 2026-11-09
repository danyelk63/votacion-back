package co.votacion.nodo;

import co.votacion.comun.ClienteRmi;
import co.votacion.comun.DireccionNodo;
import co.votacion.comun.Evento;
import co.votacion.comun.NodoVotacion;

import java.rmi.NoSuchObjectException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.List;
import java.nio.file.Path;

/**
 * Arranca un nodo: crea su RMI Registry, exporta el objeto remoto en el mismo
 * puerto y lo publica con el nombre {@link NodoVotacion#NOMBRE_RMI}.
 */
public class ServidorNodo {

    private final NodoRaft raft;
    private final ServicioNodo servicio;
    private final Registry registry;

    /**
     * @param dirDatos carpeta propia del nodo para sus archivos (ej. datos/nodo-1)
     */
    public ServidorNodo(int id, int puerto, List<DireccionNodo> pares, ConfigRaft cfg, Path dirDatos,
                        boolean silencioso) throws RemoteException {
        this.raft = new NodoRaft(id, pares.stream().map(ClienteRmi::new).toList(), cfg, dirDatos, !silencioso);
        this.registry = LocateRegistry.createRegistry(puerto);
        this.servicio = new ServicioNodo(raft, puerto); // registry y objeto comparten puerto
        registry.rebind(NodoVotacion.NOMBRE_RMI, servicio);
        raft.ev("SISTEMA", "PUBLICADO_RMI", null, "Objeto remoto publicado en el RMI Registry",
                Evento.datos("url", "rmi://" + System.getProperty("java.rmi.server.hostname", "localhost") + ":"
                        + puerto + "/" + NodoVotacion.NOMBRE_RMI));
        raft.iniciar();
    }

    public NodoRaft raft() {
        return raft;
    }

    public void detener() {
        raft.detener();
        try {
            registry.unbind(NodoVotacion.NOMBRE_RMI);
        } catch (Exception ignorada) {
        }
        try {
            UnicastRemoteObject.unexportObject(servicio, true);
            UnicastRemoteObject.unexportObject(registry, true);
        } catch (NoSuchObjectException ignorada) {
        }
    }
}

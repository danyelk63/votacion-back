package co.votacion.nodo;

import co.votacion.comun.ClienteRmi;
import co.votacion.comun.DireccionNodo;
import co.votacion.comun.NodoVotacion;

import java.rmi.NoSuchObjectException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.List;
import java.util.function.Consumer;

/**
 * Arranca un nodo: crea su RMI Registry, exporta el objeto remoto en el mismo
 * puerto y lo publica con el nombre {@link NodoVotacion#NOMBRE_RMI}.
 */
public class ServidorNodo {

    private final NodoRaft raft;
    private final ServicioNodo servicio;
    private final Registry registry;

    public ServidorNodo(int id, int puerto, List<DireccionNodo> pares, ConfigRaft cfg, boolean silencioso)
            throws RemoteException {
        Consumer<String> log = silencioso ? m -> { } : m -> System.out.println("[nodo " + id + "] " + m);
        this.raft = new NodoRaft(id, pares.stream().map(ClienteRmi::new).toList(), cfg, log);
        this.registry = LocateRegistry.createRegistry(puerto);
        this.servicio = new ServicioNodo(raft, puerto); // registry y objeto comparten puerto
        registry.rebind(NodoVotacion.NOMBRE_RMI, servicio);
        log.accept("publicado en rmi://localhost:" + puerto + "/" + NodoVotacion.NOMBRE_RMI);
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

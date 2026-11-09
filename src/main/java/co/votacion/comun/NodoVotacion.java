package co.votacion.comun;

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * Interfaz remota RMI que publica cada nodo del clúster en su RMI Registry
 * con el nombre {@link #NOMBRE_RMI}.
 *
 * <p>La usan dos tipos de clientes:
 * <ul>
 *   <li><b>Otros nodos</b> (servidor-servidor): métodos del algoritmo de consenso Raft
 *       ({@link #solicitarVoto} y {@link #anexarEntradas}).</li>
 *   <li><b>El gateway</b> (cliente-servidor): registrar votos, consultar resultados,
 *       ver el estado del nodo y simular caídas.</li>
 * </ul>
 * Todos los parámetros y retornos son {@link java.io.Serializable} porque RMI los
 * transmite por la red serializados.
 */
public interface NodoVotacion extends Remote {

    String NOMBRE_RMI = "NodoVotacion";

    // ------------------------------------------------------------ Raft (nodo <-> nodo)

    /** RPC RequestVote: un candidato pide el voto para convertirse en líder. */
    RespuestaVoto solicitarVoto(SolicitudVoto solicitud) throws RemoteException;

    /** RPC AppendEntries: el líder replica entradas del log (y sirve de latido). */
    RespuestaAnexar anexarEntradas(SolicitudAnexar solicitud) throws RemoteException;

    // ------------------------------------------------------------ API (gateway -> nodo)

    /**
     * Registra el voto de un ciudadano. Si este nodo no es el líder, reenvía la
     * petición al líder por RMI. Retorna cuando el voto quedó replicado en la
     * mayoría del clúster.
     */
    ResultadoRegistro registrarVoto(SolicitudRegistro solicitud) throws RemoteException, VotoRechazadoException;

    /** Conteo de votos. Si {@code soloLocal} es false, se consulta al líder (lectura consistente). */
    Resultados resultados(boolean soloLocal) throws RemoteException;

    /** Lista de votos registrados con la cédula enmascarada. */
    ListadoVotos votos(boolean soloLocal) throws RemoteException;

    /** Estado interno del nodo (rol, término, log...). Responde aunque el nodo esté pausado. */
    EstadoNodo estado() throws RemoteException;

    // ------------------------------------------------------------ simulación de fallos

    /** Simula la caída del nodo: deja de responder a todo salvo estado/pausar/reanudar. */
    EstadoNodo pausar() throws RemoteException;

    EstadoNodo reanudar() throws RemoteException;
}

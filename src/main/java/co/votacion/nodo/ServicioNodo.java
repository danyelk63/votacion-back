package co.votacion.nodo;

import co.votacion.comun.ClienteRmi;
import co.votacion.comun.EntradaLog;
import co.votacion.comun.EstadoNodo;
import co.votacion.comun.ListadoVotos;
import co.votacion.comun.NodoNoDisponibleException;
import co.votacion.comun.NodoVotacion;
import co.votacion.comun.RespuestaAnexar;
import co.votacion.comun.RespuestaVoto;
import co.votacion.comun.ResultadoRegistro;
import co.votacion.comun.Resultados;
import co.votacion.comun.SolicitudAnexar;
import co.votacion.comun.SolicitudRegistro;
import co.votacion.comun.SolicitudVoto;
import co.votacion.comun.VotoRechazadoException;
import co.votacion.comun.VotoRechazadoException.Codigo;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

/**
 * Objeto remoto RMI del nodo. Extiende {@link UnicastRemoteObject}, así que al
 * construirse queda exportado (escuchando llamadas remotas) en el puerto indicado.
 * Delega la lógica distribuida en {@link NodoRaft}.
 */
public class ServicioNodo extends UnicastRemoteObject implements NodoVotacion {

    private final transient NodoRaft raft;

    public ServicioNodo(NodoRaft raft, int puerto) throws RemoteException {
        super(puerto);
        this.raft = raft;
    }

    private void verificarDisponible() throws NodoNoDisponibleException {
        if (raft.estaPausado()) throw new NodoNoDisponibleException(raft.id());
    }

    // ------------------------------------------------------------ Raft

    @Override
    public RespuestaVoto solicitarVoto(SolicitudVoto solicitud) throws RemoteException {
        verificarDisponible();
        return raft.alSolicitarVoto(solicitud);
    }

    @Override
    public RespuestaAnexar anexarEntradas(SolicitudAnexar solicitud) throws RemoteException {
        verificarDisponible();
        return raft.alAnexarEntradas(solicitud);
    }

    // ------------------------------------------------------------ API

    @Override
    public ResultadoRegistro registrarVoto(SolicitudRegistro s) throws RemoteException, VotoRechazadoException {
        verificarDisponible();
        Validacion.Resultado v = Validacion.validar(s);
        if (!v.errores().isEmpty()) {
            throw new VotoRechazadoException(Codigo.DATOS_INVALIDOS, "Datos inválidos", v.errores());
        }
        try {
            EntradaLog e = raft.registrarComoLider(v.nombre(), v.cedula(), v.candidato());
            return new ResultadoRegistro("Voto registrado y replicado en la mayoría del clúster", e.indice(),
                    e.termino(), raft.id(), e.voto().registradoEn(), s.reenviadoPor());
        } catch (NodoRaft.NoEsLiderException e) {
            return reenviarAlLider(s, e.liderId);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RemoteException("Interrumpido", e);
        }
    }

    /** Un seguidor reenvía la escritura al líder por RMI: el cliente puede hablar con cualquier nodo. */
    private ResultadoRegistro reenviarAlLider(SolicitudRegistro s, Integer liderId) throws VotoRechazadoException {
        if (liderId == null || s.reenviadoPor() != null) {
            throw new VotoRechazadoException(Codigo.SIN_LIDER, "No hay líder disponible en este momento, intente de nuevo");
        }
        ClienteRmi lider = raft.par(liderId);
        try {
            return lider.llamar(n -> n.registrarVoto(s.reenviadaPor(raft.id())));
        } catch (VotoRechazadoException e) {
            throw e;
        } catch (Exception e) {
            throw new VotoRechazadoException(Codigo.SIN_LIDER, "El líder no responde, intente de nuevo");
        }
    }

    /**
     * Lecturas consistentes: si este nodo no es el líder, le pregunta al líder.
     * Si el líder no responde, contesta con su copia local e indica que puede estar desactualizada.
     */
    @Override
    public Resultados resultados(boolean soloLocal) throws RemoteException {
        verificarDisponible();
        if (!soloLocal && raft.rol() != NodoRaft.Rol.LIDER && raft.liderId() != null) {
            try {
                return raft.par(raft.liderId()).llamar(n -> n.resultados(true));
            } catch (Exception ignorada) {
                // líder inaccesible
            }
        }
        return raft.leer(m -> new Resultados(m.total(), m.resultados(), raft.id(), raft.rol() == NodoRaft.Rol.LIDER));
    }

    @Override
    public ListadoVotos votos(boolean soloLocal) throws RemoteException {
        verificarDisponible();
        if (!soloLocal && raft.rol() != NodoRaft.Rol.LIDER && raft.liderId() != null) {
            try {
                return raft.par(raft.liderId()).llamar(n -> n.votos(true));
            } catch (Exception ignorada) {
                // líder inaccesible
            }
        }
        return raft.leer(m -> new ListadoVotos(m.listado(), raft.id(), raft.rol() == NodoRaft.Rol.LIDER));
    }

    @Override
    public EstadoNodo estado() {
        return raft.estado();
    }

    // ------------------------------------------------------------ simulación de fallos

    @Override
    public EstadoNodo pausar() {
        raft.pausar();
        return raft.estado();
    }

    @Override
    public EstadoNodo reanudar() {
        raft.reanudar();
        return raft.estado();
    }
}

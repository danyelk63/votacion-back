package co.votacion.nodo;

import co.votacion.comun.ArchivoNodo;
import co.votacion.comun.ClienteRmi;
import co.votacion.comun.Evento;
import co.votacion.comun.OyenteEventos;
import co.votacion.comun.EntradaLog;
import co.votacion.comun.EstadoNodo;
import co.votacion.comun.ListadoVotos;
import co.votacion.comun.NodoNoDisponibleException;
import co.votacion.comun.NodoVotacion;
import co.votacion.comun.RespuestaAnexar;
import co.votacion.comun.RespuestaVoto;
import co.votacion.comun.ResultadoBorrado;
import co.votacion.comun.ResultadoRegistro;
import co.votacion.comun.SolicitudBorrado;
import co.votacion.comun.Resultados;
import co.votacion.comun.SolicitudAnexar;
import co.votacion.comun.SolicitudRegistro;
import co.votacion.comun.SolicitudVoto;
import co.votacion.comun.VotoRechazadoException;
import co.votacion.comun.VotoRechazadoException.Codigo;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

public class ServicioNodo extends UnicastRemoteObject implements NodoVotacion {
    private final transient NodoRaft raft;

    public ServicioNodo(NodoRaft raft, int puerto) throws RemoteException {
        super(puerto);
        this.raft = raft;
    }

    private void verificarDisponible() throws NodoNoDisponibleException {
        if (raft.estaPausado()) throw new NodoNoDisponibleException(raft.id());
    }

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

    @Override
    public ResultadoRegistro registrarVoto(SolicitudRegistro s) throws RemoteException, VotoRechazadoException {
        verificarDisponible();
        String traza = s.traza();
        raft.ev("VOTO", "RECIBIDO_RMI", traza, "Llega la invocación remota registrarVoto() desde "
                + (s.reenviadoPor() == null ? "el gateway" : "el nodo " + s.reenviadoPor()),
                Evento.datos("nombre", s.nombre(), "cedula", Evento.enmascarar(s.cedula()), "candidato", s.candidato()));

        Validacion.Resultado v = Validacion.validar(s);
        if (!v.errores().isEmpty()) {
            raft.ev("VOTO", "VALIDACION", traza, "Datos inválidos: " + String.join("; ", v.errores()), null);
            throw new VotoRechazadoException(Codigo.DATOS_INVALIDOS, "Datos inválidos", v.errores());
        }
        raft.ev("VOTO", "VALIDACION", traza, "Valida y normaliza los datos (nombre, cédula de 6 a 10 dígitos, candidato): OK",
                Evento.datos("cedulaNormalizada", Evento.enmascarar(v.cedula())));
        try {
            EntradaLog e = raft.registrarComoLider(v.nombre(), v.cedula(), v.candidato(), traza);
            return new ResultadoRegistro("Voto registrado y replicado en la mayoría del clúster", e.indice(),
                    e.termino(), raft.id(), e.voto().registradoEn(), s.reenviadoPor());
        } catch (NodoRaft.NoEsLiderException e) {
            return reenviarAlLider(s, e.liderId);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RemoteException("Interrumpido", e);
        }
    }

    private ResultadoRegistro reenviarAlLider(SolicitudRegistro s, Integer liderId) throws VotoRechazadoException {
        if (liderId == null || s.reenviadoPor() != null) {
            raft.ev("VOTO", "SIN_LIDER", s.traza(), "No es el líder y no conoce un líder vigente: pide reintentar", null);
            throw new VotoRechazadoException(Codigo.SIN_LIDER, "No hay líder disponible en este momento, intente de nuevo");
        }
        ClienteRmi lider = raft.par(liderId);
        raft.ev("VOTO", "REENVIO_LIDER", s.traza(), "No es el líder: solo el líder puede escribir. Reenvía la petición al nodo "
                + liderId + " por RMI", Evento.datos("destino", lider.direccion()));
        try {
            ResultadoRegistro r = lider.llamar(n -> n.registrarVoto(s.reenviadaPor(raft.id())));
            raft.ev("VOTO", "RESPUESTA", s.traza(), "El líder confirmó el voto; devuelve la respuesta al gateway", null);
            return r;
        } catch (VotoRechazadoException e) {
            raft.ev("VOTO", "RESPUESTA", s.traza(), "El líder rechazó el voto: " + e.getMessage(), null);
            throw e;
        } catch (Exception e) {
            raft.ev("RED", "LIDER_INACCESIBLE", s.traza(), "El líder (nodo " + liderId + ") no responde por RMI", null);
            throw new VotoRechazadoException(Codigo.SIN_LIDER, "El líder no responde, intente de nuevo");
        }
    }

    @Override
    public ResultadoBorrado borrarVotos(SolicitudBorrado s) throws RemoteException, VotoRechazadoException {
        verificarDisponible();
        raft.ev("VOTO", "RECIBIDO_RMI", s.traza(), "Llega la invocación remota borrarVotos() desde "
                + (s.reenviadoPor() == null ? "el gateway" : "el nodo " + s.reenviadoPor()), null);
        try {
            Object[] r = raft.borrarComoLider(s.traza());
            EntradaLog e = (EntradaLog) r[0];
            return new ResultadoBorrado("Votos borrados en todos los nodos del clúster", (Integer) r[1], e.indice(),
                    e.termino(), raft.id(), s.reenviadoPor());
        } catch (NodoRaft.NoEsLiderException e) {
            Integer liderId = e.liderId;
            if (liderId == null || s.reenviadoPor() != null) {
                raft.ev("VOTO", "SIN_LIDER", s.traza(), "No es el líder y no conoce un líder vigente: pide reintentar", null);
                throw new VotoRechazadoException(Codigo.SIN_LIDER, "No hay líder disponible en este momento, intente de nuevo");
            }
            ClienteRmi lider = raft.par(liderId);
            raft.ev("VOTO", "REENVIO_LIDER", s.traza(), "No es el líder: solo el líder puede escribir. Reenvía la orden al nodo "
                    + liderId + " por RMI", Evento.datos("destino", lider.direccion()));
            try {
                return lider.llamar(n -> n.borrarVotos(s.reenviadaPor(raft.id())));
            } catch (VotoRechazadoException ex) {
                throw ex;
            } catch (Exception ex) {
                raft.ev("RED", "LIDER_INACCESIBLE", s.traza(), "El líder (nodo " + liderId + ") no responde por RMI", null);
                throw new VotoRechazadoException(Codigo.SIN_LIDER, "El líder no responde, intente de nuevo");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RemoteException("Interrumpido", e);
        }
    }

    @Override
    public Resultados resultados(boolean soloLocal) throws RemoteException {
        verificarDisponible();
        if (!soloLocal && raft.rol() != NodoRaft.Rol.LIDER && raft.liderId() != null) {
            try {
                return raft.par(raft.liderId()).llamar(n -> n.resultados(true));
            } catch (Exception ignorada) {
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
            }
        }
        return raft.leer(m -> new ListadoVotos(m.listado(), raft.id(), raft.rol() == NodoRaft.Rol.LIDER));
    }

    @Override
    public EstadoNodo estado() {
        return raft.estado();
    }

    @Override
    public void suscribir(OyenteEventos oyente) {
        raft.suscribir(oyente);
    }

    @Override
    public ArchivoNodo leerArchivo() {
        return raft.leerArchivo();
    }

    @Override
    public EstadoNodo pausar() {
        raft.pausar();
        return raft.estado();
    }

    @Override
    public EstadoNodo reiniciarTodo() {
        raft.reiniciarTodo();
        return raft.estado();
    }

    @Override
    public EstadoNodo reanudar() {
        raft.reanudar();
        return raft.estado();
    }
}

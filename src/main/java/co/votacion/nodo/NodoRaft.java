package co.votacion.nodo;

import co.votacion.comun.ClienteRmi;
import co.votacion.comun.EntradaLog;
import co.votacion.comun.ArchivoNodo;
import co.votacion.comun.EstadoNodo;
import co.votacion.comun.Evento;
import co.votacion.comun.OyenteEventos;
import co.votacion.comun.RespuestaAnexar;
import co.votacion.comun.RespuestaVoto;
import co.votacion.comun.SolicitudAnexar;
import co.votacion.comun.SolicitudVoto;
import co.votacion.comun.Voto;
import co.votacion.comun.VotoRechazadoException;
import co.votacion.comun.VotoRechazadoException.Codigo;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

public class NodoRaft {
    public enum Rol { SEGUIDOR, CANDIDATO, LIDER }

    static class NoEsLiderException extends Exception {
        final Integer liderId;

        NoEsLiderException(Integer liderId) {
            super("No soy el líder");
            this.liderId = liderId;
        }
    }

    private record Espera(String idEntrada, CompletableFuture<EntradaLog> futuro) {
    }

    private static final int MAX_ENTRADAS_POR_RPC = 100;

    private final int id;
    private final Map<Integer, ClienteRmi> pares = new LinkedHashMap<>();
    private final ConfigRaft cfg;
    private final Bitacora bitacora;
    private final AlmacenVotos almacen;
    private final Set<Integer> paresInaccesibles = ConcurrentHashMap.newKeySet();

    private long terminoActual = 0;
    private Integer votoPara = null;
    private final List<EntradaLog> registro = new ArrayList<>(List.of(EntradaLog.centinela()));

    private int indiceConfirmado = 0;
    private int ultimoAplicado = 0;
    private Rol rol = Rol.SEGUIDOR;
    private Integer liderId = null;
    private volatile boolean pausado = false;

    private final Map<Integer, Integer> siguienteIndice = new HashMap<>();
    private final Map<Integer, Integer> indiceCoincidente = new HashMap<>();
    private final Set<Integer> enVuelo = ConcurrentHashMap.newKeySet();

    private final RegistroVotos maquina = new RegistroVotos();
    private final Map<Integer, Espera> esperas = new HashMap<>();

    private final ScheduledExecutorService planificador;
    private final ExecutorService rpcs;
    private ScheduledFuture<?> temporizadorEleccion;
    private ScheduledFuture<?> temporizadorLatido;

    public NodoRaft(int id, List<ClienteRmi> pares, ConfigRaft cfg, Path dirDatos, boolean consola) {
        this.id = id;
        pares.forEach(p -> this.pares.put(p.id(), p));
        this.cfg = cfg;
        this.bitacora = new Bitacora(dirDatos.resolve("eventos.log"), consola);
        this.almacen = new AlmacenVotos(id, dirDatos.resolve("votos.txt"));
        this.planificador = Executors.newSingleThreadScheduledExecutor(hilo("raft-" + id + "-timer"));
        this.rpcs = Executors.newCachedThreadPool(hilo("raft-" + id + "-rpc"));
    }

    public synchronized void iniciar() {
        ev("SISTEMA", "INICIO", null, "Nodo iniciado como SEGUIDOR en un clúster de " + tamanoCluster() + " nodos",
                Evento.datos("votos", almacen.archivo().toAbsolutePath(), "eventos", bitacora.archivo().toAbsolutePath()));
        reiniciarTemporizadorEleccion();
    }

    public synchronized void detener() {
        cancelar(temporizadorEleccion);
        cancelar(temporizadorLatido);
        planificador.shutdownNow();
        rpcs.shutdownNow();
        bitacora.detener();
    }

    public synchronized void pausar() {
        pausado = true;
        cancelar(temporizadorEleccion);
        cancelar(temporizadorLatido);
        ev("SISTEMA", "PAUSADO", null, "Nodo PAUSADO: simula una caída, deja de responder a otros nodos y al gateway", null);
    }

    public synchronized void reanudar() {
        if (!pausado) return;
        pausado = false;

        if (rol == Rol.LIDER) iniciarLatidos();
        else reiniciarTemporizadorEleccion();
        ev("SISTEMA", "REANUDADO", null, "Nodo REANUDADO: vuelve a participar en el clúster", null);
    }

    public boolean estaPausado() {
        return pausado;
    }

    public int id() {
        return id;
    }

    synchronized int tamanoCluster() {
        return pares.size() + 1;
    }

    private int mayoria() {
        return tamanoCluster() / 2 + 1;
    }

    private int ultimoIndice() {
        return registro.size() - 1;
    }

    private long ultimoTermino() {
        return registro.get(ultimoIndice()).termino();
    }

    public synchronized Rol rol() {
        return rol;
    }

    public synchronized Integer liderId() {
        return liderId;
    }

    public synchronized long termino() {
        return terminoActual;
    }

    ClienteRmi par(int idPar) {
        return pares.get(idPar);
    }

    public synchronized <T> T leer(java.util.function.Function<RegistroVotos, T> lectura) {
        return lectura.apply(maquina);
    }

    public synchronized List<String> idsConfirmados() {
        return registro.subList(1, indiceConfirmado + 1).stream().map(EntradaLog::id).toList();
    }

    public synchronized EstadoNodo estado() {
        Map<Integer, Integer> replicacion = null;
        if (rol == Rol.LIDER) {
            replicacion = new LinkedHashMap<>();
            for (Integer p : pares.keySet()) replicacion.put(p, indiceCoincidente.getOrDefault(p, 0));
        }
        return new EstadoNodo(id, rol.name(), terminoActual, liderId, votoPara, pausado, ultimoIndice(),
                indiceConfirmado, ultimoAplicado, maquina.total(), replicacion);
    }

    private void reiniciarTemporizadorEleccion() {
        cancelar(temporizadorEleccion);
        if (pausado) return;
        long espera = ThreadLocalRandom.current().nextLong(cfg.eleccionMinMs(), cfg.eleccionMaxMs() + 1);
        temporizadorEleccion = planificador.schedule(this::iniciarEleccion, espera, TimeUnit.MILLISECONDS);
    }

    private synchronized void iniciarEleccion() {
        if (pausado || rol == Rol.LIDER) return;
        rol = Rol.CANDIDATO;
        terminoActual++;
        votoPara = id;
        liderId = null;
        final long termino = terminoActual;
        ev("ELECCION", "CANDIDATURA", null, "No recibió latidos del líder a tiempo: se postula como CANDIDATO y pide votos por RMI (solicitarVoto)",
                Evento.datos("termino", termino, "mayoriaNecesaria", mayoria()));
        reiniciarTemporizadorEleccion();

        AtomicInteger votos = new AtomicInteger(1);
        if (votos.get() >= mayoria()) {
            convertirseEnLider();
            return;
        }
        SolicitudVoto solicitud = new SolicitudVoto(termino, id, ultimoIndice(), ultimoTermino());
        for (ClienteRmi par : pares.values()) {
            rpcs.submit(() -> {
                try {
                    RespuestaVoto r = par.llamar(n -> n.solicitarVoto(solicitud));
                    synchronized (this) {
                        if (pausado) return;
                        if (r.termino() > terminoActual) {
                            retrocederASeguidor(r.termino(), null);
                            return;
                        }
                        if (rol != Rol.CANDIDATO || terminoActual != termino) return;
                        if (r.votoConcedido()) {
                            int total = votos.incrementAndGet();
                            ev("ELECCION", "VOTO_RECIBIDO", null, "El nodo " + par.id() + " le concedió su voto ("
                                    + total + "/" + tamanoCluster() + ", necesita " + mayoria() + ")", null);
                            if (total >= mayoria()) convertirseEnLider();
                        } else {
                            ev("ELECCION", "VOTO_NEGADO", null, "El nodo " + par.id() + " le negó su voto", null);
                        }
                    }
                } catch (Exception e) {
                }
            });
        }
    }

    private void convertirseEnLider() {
        if (rol == Rol.LIDER) return;
        rol = Rol.LIDER;
        liderId = id;
        cancelar(temporizadorEleccion);
        for (Integer p : pares.keySet()) {
            siguienteIndice.put(p, ultimoIndice() + 1);
            indiceCoincidente.put(p, 0);
        }
        ev("ELECCION", "NUEVO_LIDER", null, "Ganó la elección: ahora es el LÍDER del término " + terminoActual, null);

        anexar(EntradaLog.Tipo.NOOP, null, null);
        avanzarConfirmacion();
        iniciarLatidos();
    }

    private void iniciarLatidos() {
        cancelar(temporizadorLatido);
        temporizadorLatido = planificador.scheduleAtFixedRate(this::replicarATodos, 0, cfg.latidoMs(),
                TimeUnit.MILLISECONDS);
    }

    private void retrocederASeguidor(long termino, Integer nuevoLider) {
        if (termino > terminoActual) {
            terminoActual = termino;
            votoPara = null;
        }
        Rol anterior = rol;
        rol = Rol.SEGUIDOR;
        if (anterior != Rol.SEGUIDOR) {
            ev("ELECCION", "A_SEGUIDOR", null, "Deja de ser " + anterior + ": encontró un término mayor o un líder válido", null);
        }
        if (nuevoLider != null) liderId = nuevoLider;
        cancelar(temporizadorLatido);
        reiniciarTemporizadorEleccion();
    }

    public synchronized RespuestaVoto alSolicitarVoto(SolicitudVoto s) {
        if (s.termino() > terminoActual) retrocederASeguidor(s.termino(), null);

        boolean logActualizado = s.ultimoTerminoLog() > ultimoTermino()
                || (s.ultimoTerminoLog() == ultimoTermino() && s.ultimoIndiceLog() >= ultimoIndice());
        boolean conceder = s.termino() == terminoActual
                && (votoPara == null || votoPara == s.candidatoId())
                && logActualizado;

        if (conceder) {
            votoPara = s.candidatoId();
            reiniciarTemporizadorEleccion();
            ev("ELECCION", "VOTO_CONCEDIDO", null, "Concede su voto al candidato nodo " + s.candidatoId(),
                    Evento.datos("terminoCandidato", s.termino()));
        } else if (s.termino() >= terminoActual) {
            ev("ELECCION", "VOTO_NEGADO", null, "Niega su voto al nodo " + s.candidatoId() + ": "
                    + (!logActualizado ? "su log está menos actualizado" : "ya votó por el nodo " + votoPara + " en este término"), null);
        }
        return new RespuestaVoto(terminoActual, conceder);
    }

    private EntradaLog anexar(EntradaLog.Tipo tipo, Voto voto, String traza) {
        EntradaLog e = new EntradaLog(ultimoIndice() + 1, terminoActual, UUID.randomUUID().toString(), tipo, voto, traza);
        registro.add(e);
        return e;
    }

    private synchronized void replicarATodos() {
        if (rol != Rol.LIDER || pausado) return;
        pares.values().forEach(this::replicarA);
    }

    private void replicarA(ClienteRmi par) {
        if (!enVuelo.add(par.id())) return;
        final long termino = terminoActual;
        final int siguiente = siguienteIndice.get(par.id());
        final int previo = siguiente - 1;
        List<EntradaLog> entradas = new ArrayList<>(
                registro.subList(siguiente, Math.min(registro.size(), siguiente + MAX_ENTRADAS_POR_RPC)));
        SolicitudAnexar solicitud = new SolicitudAnexar(termino, id, previo, registro.get(previo).termino(),
                entradas, indiceConfirmado);
        for (EntradaLog e : entradas) {
            if (e.rastreable()) {
                ev("REPLICACION", "ENVIO", e.traza(), "Envía la " + e.descripcion()
                        + " al nodo " + par.id() + " (anexarEntradas por RMI)", Evento.datos("destino", par.id()));
            }
        }

        rpcs.submit(() -> {
            boolean pendiente = false;
            try {
                RespuestaAnexar r = par.llamar(n -> n.anexarEntradas(solicitud));
                synchronized (this) {
                    if (pausado) return;
                    if (paresInaccesibles.remove(par.id())) {
                        ev("RED", "PAR_RECUPERADO", null, "El nodo " + par.id() + " vuelve a responder por RMI", null);
                    }
                    if (r.termino() > terminoActual) {
                        retrocederASeguidor(r.termino(), null);
                        return;
                    }
                    if (rol != Rol.LIDER || terminoActual != termino) return;
                    if (r.exito()) {
                        for (EntradaLog e : entradas) {
                            if (e.rastreable()) {
                                ev("REPLICACION", "ACK", e.traza(), "El nodo " + par.id()
                                        + " confirma que guardó la " + e.descripcion() + " en su log",
                                        Evento.datos("seguidor", par.id()));
                            }
                        }
                        indiceCoincidente.merge(par.id(), r.indiceCoincidente(), Math::max);
                        siguienteIndice.put(par.id(), indiceCoincidente.get(par.id()) + 1);
                        avanzarConfirmacion();
                    } else {
                        siguienteIndice.put(par.id(), Math.max(1, Math.min(r.indiceConflicto(), previo)));
                        ev("REPLICACION", "INCONSISTENCIA", null, "El log del nodo " + par.id()
                                + " no coincide en el índice " + previo + ": se reintentará desde el "
                                + siguienteIndice.get(par.id()), null);
                    }
                    pendiente = siguienteIndice.get(par.id()) <= ultimoIndice();
                }
            } catch (Exception e) {
                if (paresInaccesibles.add(par.id())) {
                    ev("RED", "PAR_CAIDO", null, "El nodo " + par.id() + " no responde a las llamadas RMI (caído o pausado)",
                            Evento.datos("error", e.getClass().getSimpleName()));
                }
            } finally {
                enVuelo.remove(par.id());
            }
            if (pendiente) {
                synchronized (this) {
                    if (rol == Rol.LIDER && !pausado) replicarA(par);
                }
            }
        });
    }

    private void avanzarConfirmacion() {
        for (int n = ultimoIndice(); n > indiceConfirmado; n--) {
            if (registro.get(n).termino() != terminoActual) break;
            final int indice = n;
            long replicas = 1 + pares.keySet().stream()
                    .filter(p -> indiceCoincidente.getOrDefault(p, 0) >= indice).count();
            if (replicas >= mayoria()) {
                for (int k = indiceConfirmado + 1; k <= n; k++) {
                    EntradaLog e = registro.get(k);
                    if (e.rastreable()) {
                        ev("VOTO", "CONFIRMADO", e.traza(), "La " + e.descripcion() + " queda CONFIRMADA: ya está en "
                                + replicas + " de " + tamanoCluster() + " nodos (mayoría = " + mayoria() + ")", null);
                    }
                }
                indiceConfirmado = n;
                aplicarConfirmadas();
                break;
            }
        }
    }

    public synchronized RespuestaAnexar alAnexarEntradas(SolicitudAnexar s) {
        if (s.termino() < terminoActual) return new RespuestaAnexar(terminoActual, false, 0, 0);

        if (s.termino() > terminoActual || rol != Rol.SEGUIDOR) retrocederASeguidor(s.termino(), s.liderId());
        if (liderId == null || liderId != s.liderId()) {
            ev("ELECCION", "RECONOCE_LIDER", null, "Reconoce al nodo " + s.liderId() + " como LÍDER del término " + s.termino(), null);
        }
        liderId = s.liderId();
        reiniciarTemporizadorEleccion();

        if (s.indiceLogPrevio() > ultimoIndice()) {
            return new RespuestaAnexar(terminoActual, false, 0, ultimoIndice() + 1);
        }
        if (registro.get(s.indiceLogPrevio()).termino() != s.terminoLogPrevio()) {
            long terminoConflicto = registro.get(s.indiceLogPrevio()).termino();
            int i = s.indiceLogPrevio();
            while (i > 1 && registro.get(i - 1).termino() == terminoConflicto) i--;
            return new RespuestaAnexar(terminoActual, false, 0, i);
        }

        for (EntradaLog entrada : s.entradas()) {
            int i = entrada.indice();
            if (i <= ultimoIndice() && registro.get(i).termino() != entrada.termino()) truncarDesde(i);
            if (i > ultimoIndice()) {
                registro.add(entrada);
                if (entrada.rastreable()) {
                    ev("REPLICACION", "ENTRADA_RECIBIDA", entrada.traza(), "Recibe del líder (nodo "
                            + s.liderId() + ") la " + entrada.descripcion() + " y la agrega a su log (aún sin confirmar)", null);
                }
            }
        }

        int ultimoNuevo = s.indiceLogPrevio() + s.entradas().size();
        if (s.confirmadoLider() > indiceConfirmado) {
            indiceConfirmado = Math.min(s.confirmadoLider(), ultimoNuevo);
            aplicarConfirmadas();
        }
        return new RespuestaAnexar(terminoActual, true, ultimoNuevo, 0);
    }

    private void truncarDesde(int indice) {
        ev("REPLICACION", "TRUNCADO", null, "Descarta entradas NO confirmadas desde el índice " + indice
                + " porque no coinciden con el log del líder", null);
        registro.subList(indice, registro.size()).clear();
        esperas.entrySet().removeIf(e -> {
            if (e.getKey() < indice) return false;
            e.getValue().futuro().completeExceptionally(new VotoRechazadoException(Codigo.NO_CONFIRMADO,
                    "El voto no alcanzó a confirmarse porque cambió el líder, intente de nuevo"));
            return true;
        });
    }

    private void aplicarConfirmadas() {
        while (ultimoAplicado < indiceConfirmado) {
            ultimoAplicado++;
            EntradaLog entrada = registro.get(ultimoAplicado);
            if (entrada.tipo() == EntradaLog.Tipo.REINICIO) {
                aplicarBorrado(entrada);
                Espera espera = esperas.remove(entrada.indice());
                if (espera != null) {
                    if (espera.idEntrada().equals(entrada.id())) espera.futuro().complete(entrada);
                    else espera.futuro().completeExceptionally(new VotoRechazadoException(Codigo.NO_CONFIRMADO,
                            "La orden fue reemplazada por otro líder, intente de nuevo"));
                }
                continue;
            }
            boolean aplicado = maquina.aplicar(entrada);
            if (entrada.tipo() == EntradaLog.Tipo.VOTO) {
                String traza = entrada.traza();
                if (aplicado) {
                    ev("VOTO", "APLICADO", traza, "Aplica la entrada #" + entrada.indice()
                            + " al registro electoral en memoria", Evento.datos("conteo", maquina.conteo()));
                    try {
                        int linea = almacen.guardar(entrada);
                        ev("PERSISTENCIA", "GUARDADO_ARCHIVO", traza, "Guarda el voto en el archivo de texto del servidor",
                                Evento.datos("archivo", almacen.archivo().toAbsolutePath(), "registro", linea));
                    } catch (IOException ex) {
                        ev("PERSISTENCIA", "ERROR_ARCHIVO", traza, "No se pudo escribir el archivo: " + ex.getMessage(), null);
                    }
                } else {
                    ev("VOTO", "RECHAZADO", traza, "Entrada #" + entrada.indice() + " descartada: la cédula ya había votado", null);
                }
            }
            Espera espera = esperas.remove(entrada.indice());
            if (espera == null) continue;
            if (!espera.idEntrada().equals(entrada.id())) {
                espera.futuro().completeExceptionally(new VotoRechazadoException(Codigo.NO_CONFIRMADO,
                        "El voto fue reemplazado por otro líder, intente de nuevo"));
            } else if (!aplicado) {
                espera.futuro().completeExceptionally(new VotoRechazadoException(Codigo.CEDULA_DUPLICADA,
                        "Esta cédula ya registró su voto"));
            } else {
                espera.futuro().complete(entrada);
            }
        }
    }

    private void aplicarBorrado(EntradaLog entrada) {
        int votos = maquina.total();
        maquina.reiniciar();
        ultimosBorrados.put(entrada.indice(), votos);
        ev("VOTO", "BORRADO", entrada.traza(), "Aplica la orden de borrado #" + entrada.indice()
                + ": elimina " + votos + " voto(s) del registro electoral en memoria", Evento.datos("conteo", maquina.conteo()));
        try {
            almacen.reiniciar(entrada);
            ev("PERSISTENCIA", "ARCHIVO_REINICIADO", entrada.traza(), "Vacía el archivo de votos del servidor",
                    Evento.datos("archivo", almacen.archivo().toAbsolutePath(), "votosBorrados", votos));
        } catch (IOException ex) {
            ev("PERSISTENCIA", "ERROR_ARCHIVO", entrada.traza(), "No se pudo reiniciar el archivo: " + ex.getMessage(), null);
        }
    }

    private final Map<Integer, Integer> ultimosBorrados = new HashMap<>();

    Object[] borrarComoLider(String traza) throws NoEsLiderException, VotoRechazadoException, InterruptedException {
        long inicio = System.nanoTime();
        CompletableFuture<EntradaLog> futuro = new CompletableFuture<>();
        int indice;
        synchronized (this) {
            if (rol != Rol.LIDER || pausado) throw new NoEsLiderException(liderId);
            EntradaLog entrada = anexar(EntradaLog.Tipo.REINICIO, null, traza);
            indice = entrada.indice();
            esperas.put(indice, new Espera(entrada.id(), futuro));
            ev("VOTO", "ANEXADO_LOG", traza, "Agrega la orden de BORRAR TODOS LOS VOTOS a su log como entrada #" + indice
                    + " (término " + terminoActual + ") y la replica a los seguidores", Evento.datos("indice", indice));
            avanzarConfirmacion();
            replicarATodos();
        }
        EntradaLog confirmada = esperarConfirmacion(futuro, indice, traza, inicio, "la orden de borrado");
        synchronized (this) {
            return new Object[] {confirmada, ultimosBorrados.getOrDefault(confirmada.indice(), 0)};
        }
    }

    private EntradaLog esperarConfirmacion(CompletableFuture<EntradaLog> futuro, int indice, String traza, long inicio,
                                           String que) throws VotoRechazadoException, InterruptedException {
        try {
            EntradaLog confirmada = futuro.get(cfg.timeoutConfirmacionMs(), TimeUnit.MILLISECONDS);
            ev("VOTO", "RESPUESTA", traza, "Responde ÉXITO a quien hizo la llamada RMI",
                    Evento.datos("duracionMs", (System.nanoTime() - inicio) / 1_000_000));
            return confirmada;
        } catch (TimeoutException e) {
            synchronized (this) {
                esperas.remove(indice);
            }
            ev("VOTO", "RECHAZADO", traza, "No logró la mayoría en " + cfg.timeoutConfirmacionMs()
                    + " ms: hay demasiados nodos caídos", null);
            throw new VotoRechazadoException(Codigo.TIMEOUT,
                    "No se logró replicar " + que + " en la mayoría del clúster a tiempo");
        } catch (ExecutionException e) {
            if (e.getCause() instanceof VotoRechazadoException v) throw v;
            throw new IllegalStateException(e.getCause());
        }
    }

    EntradaLog registrarComoLider(String nombre, String cedula, String candidato, String traza)
            throws NoEsLiderException, VotoRechazadoException, InterruptedException {
        long inicio = System.nanoTime();
        CompletableFuture<EntradaLog> futuro = new CompletableFuture<>();
        int indice;
        synchronized (this) {
            if (rol != Rol.LIDER || pausado) throw new NoEsLiderException(liderId);

            boolean pendienteMismaCedula = registro.subList(indiceConfirmado + 1, registro.size()).stream()
                    .anyMatch(e -> e.tipo() == EntradaLog.Tipo.VOTO && e.voto().cedula().equals(cedula));
            if (maquina.yaVoto(cedula) || pendienteMismaCedula) {
                ev("VOTO", "RECHAZADO", traza, "Cédula duplicada: " + Evento.enmascarar(cedula) + " ya tiene un voto "
                        + (pendienteMismaCedula ? "en proceso" : "confirmado"), null);
                throw new VotoRechazadoException(Codigo.CEDULA_DUPLICADA, "Esta cédula ya registró su voto");
            }
            ev("VOTO", "VERIFICACION", traza, "Verifica en el registro que la cédula " + Evento.enmascarar(cedula)
                    + " no haya votado: OK", null);

            Voto voto = new Voto(nombre, cedula, candidato, java.time.Instant.now().toString(), id, traza);
            EntradaLog entrada = anexar(EntradaLog.Tipo.VOTO, voto, traza);
            indice = entrada.indice();
            esperas.put(indice, new Espera(entrada.id(), futuro));
            ev("VOTO", "ANEXADO_LOG", traza, "Agrega el voto a su log como entrada #" + indice
                    + " (término " + terminoActual + ") y lo replica a los seguidores",
                    Evento.datos("indice", indice, "candidato", candidato));
            avanzarConfirmacion();
            replicarATodos();
        }
        return esperarConfirmacion(futuro, indice, traza, inicio, "el voto");
    }

    synchronized void ev(String categoria, String etapa, String traza, String mensaje, java.util.Map<String, String> datos) {
        bitacora.publicar(Evento.nuevo("NODO", id, rol.name(), terminoActual, categoria, etapa, traza, mensaje, datos));
    }

    void suscribir(OyenteEventos oyente) {
        if (bitacora.suscribir(oyente)) {
            ev("SISTEMA", "SUSCRIPCION", null, "El gateway se suscribió a los eventos de este nodo (callback RMI)", null);
        }
    }

    synchronized void reiniciarTodo() {
        cancelar(temporizadorEleccion);
        cancelar(temporizadorLatido);
        terminoActual = 0;
        votoPara = null;
        registro.clear();
        registro.add(EntradaLog.centinela());
        indiceConfirmado = 0;
        ultimoAplicado = 0;
        rol = Rol.SEGUIDOR;
        liderId = null;
        siguienteIndice.clear();
        indiceCoincidente.clear();
        paresInaccesibles.clear();
        ultimosBorrados.clear();
        esperas.values().forEach(e -> e.futuro().completeExceptionally(
                new VotoRechazadoException(Codigo.NO_CONFIRMADO, "El sistema se reinició")));
        esperas.clear();
        maquina.reiniciar();
        bitacora.vaciarArchivo();
        try {
            almacen.reiniciarDesdeCero();
        } catch (IOException ignorada) {
        }
        ev("SISTEMA", "REINICIO_TOTAL", null, "Estado reiniciado de cero: log vacío, término 0, sin votos ni registros", null);
        if (!pausado) reiniciarTemporizadorEleccion();
    }

    ArchivoNodo leerArchivo() {
        return almacen.leer(200);
    }

    private static void cancelar(ScheduledFuture<?> f) {
        if (f != null) f.cancel(false);
    }

    private static java.util.concurrent.ThreadFactory hilo(String nombre) {
        return r -> {
            Thread t = new Thread(r, nombre);
            t.setDaemon(true);
            return t;
        };
    }
}

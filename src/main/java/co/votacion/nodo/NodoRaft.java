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

/**
 * Implementación didáctica del algoritmo de consenso Raft
 * (Ongaro y Ousterhout, "In Search of an Understandable Consensus Algorithm", 2014),
 * usando Java RMI como transporte entre nodos.
 *
 * <p>Cubre:
 * <ul>
 *   <li>Elección de líder con timeouts aleatorios y términos ({@code solicitarVoto}).</li>
 *   <li>Replicación del log desde el líder ({@code anexarEntradas}) con verificación de consistencia.</li>
 *   <li>Confirmación (commit) por mayoría y aplicación ordenada a la máquina de estados.</li>
 *   <li>Restricción de elección: solo gana un candidato con el log al menos tan actualizado.</li>
 *   <li>Entrada "noop" al asumir el liderazgo para confirmar entradas de términos anteriores.</li>
 * </ul>
 *
 * <p>Simplificaciones (ejercicio académico, sin base de datos): el estado vive en
 * memoria (un nodo reiniciado vuelve vacío y el líder le reenvía todo el log) y la
 * membresía del clúster es fija.
 *
 * <p>Concurrencia: todo el estado se protege con el monitor de {@code this}; las
 * llamadas RMI a otros nodos se hacen SIN tener el candado, en un pool de hilos.
 */
public class NodoRaft {

    public enum Rol { SEGUIDOR, CANDIDATO, LIDER }

    /** Señal interna: la petición llegó a un nodo que no es líder. */
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

    // Estado "persistente" de Raft (en memoria)
    private long terminoActual = 0;
    private Integer votoPara = null;
    private final List<EntradaLog> registro = new ArrayList<>(List.of(EntradaLog.centinela()));

    // Estado volátil
    private int indiceConfirmado = 0;
    private int ultimoAplicado = 0;
    private Rol rol = Rol.SEGUIDOR;
    private Integer liderId = null;
    private volatile boolean pausado = false;

    // Estado del líder
    private final Map<Integer, Integer> siguienteIndice = new HashMap<>();
    private final Map<Integer, Integer> indiceCoincidente = new HashMap<>();
    private final Set<Integer> enVuelo = ConcurrentHashMap.newKeySet();

    private final RegistroVotos maquina = new RegistroVotos();
    private final Map<Integer, Espera> esperas = new HashMap<>();

    private final ScheduledExecutorService planificador;
    private final ExecutorService rpcs;
    private ScheduledFuture<?> temporizadorEleccion;
    private ScheduledFuture<?> temporizadorLatido;

    /**
     * @param dirDatos carpeta donde el nodo escribe sus archivos (votos.txt y eventos.log)
     * @param consola  si imprime los eventos en la salida estándar
     */
    public NodoRaft(int id, List<ClienteRmi> pares, ConfigRaft cfg, Path dirDatos, boolean consola) {
        this.id = id;
        pares.forEach(p -> this.pares.put(p.id(), p));
        this.cfg = cfg;
        this.bitacora = new Bitacora(dirDatos.resolve("eventos.log"), consola);
        this.almacen = new AlmacenVotos(id, dirDatos.resolve("votos.txt"));
        this.planificador = Executors.newSingleThreadScheduledExecutor(hilo("raft-" + id + "-timer"));
        this.rpcs = Executors.newCachedThreadPool(hilo("raft-" + id + "-rpc"));
    }

    // ================================================================== ciclo de vida

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

    /** Simula una caída / partición de red: el nodo deja de participar. */
    public synchronized void pausar() {
        pausado = true;
        cancelar(temporizadorEleccion);
        cancelar(temporizadorLatido);
        ev("SISTEMA", "PAUSADO", null, "Nodo PAUSADO: simula una caída, deja de responder a otros nodos y al gateway", null);
    }

    public synchronized void reanudar() {
        if (!pausado) return;
        pausado = false;
        // Si era líder, sigue enviando latidos; si ya hay otro líder con término mayor,
        // la primera respuesta lo hará retroceder a seguidor.
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

    // ================================================================== consultas

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

    /** Lectura del estado de la máquina bajo el candado. */
    public synchronized <T> T leer(java.util.function.Function<RegistroVotos, T> lectura) {
        return lectura.apply(maquina);
    }

    /** Ids de las entradas confirmadas, en orden (para pruebas). */
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

    // ================================================================== elección de líder

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
        reiniciarTemporizadorEleccion(); // si nadie gana (voto dividido), se reintenta

        AtomicInteger votos = new AtomicInteger(1); // su propio voto
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
                    // par inaccesible: no vota
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
        // Entrada noop: permite confirmar entradas de términos anteriores (§5.4.2 del paper).
        anexar(EntradaLog.Tipo.NOOP, null);
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

    /** RPC RequestVote, lado receptor. */
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

    // ================================================================== replicación del log

    private EntradaLog anexar(EntradaLog.Tipo tipo, Voto voto) {
        EntradaLog e = new EntradaLog(ultimoIndice() + 1, terminoActual, UUID.randomUUID().toString(), tipo, voto);
        registro.add(e);
        return e;
    }

    private synchronized void replicarATodos() {
        if (rol != Rol.LIDER || pausado) return;
        pares.values().forEach(this::replicarA);
    }

    /** Envía a un seguidor las entradas que le faltan (o un latido vacío). Se llama con el candado. */
    private void replicarA(ClienteRmi par) {
        if (!enVuelo.add(par.id())) return; // ya hay una llamada en curso a ese par
        final long termino = terminoActual;
        final int siguiente = siguienteIndice.get(par.id());
        final int previo = siguiente - 1;
        List<EntradaLog> entradas = new ArrayList<>(
                registro.subList(siguiente, Math.min(registro.size(), siguiente + MAX_ENTRADAS_POR_RPC)));
        SolicitudAnexar solicitud = new SolicitudAnexar(termino, id, previo, registro.get(previo).termino(),
                entradas, indiceConfirmado);
        for (EntradaLog e : entradas) {
            if (e.tipo() == EntradaLog.Tipo.VOTO) {
                ev("REPLICACION", "ENVIO", e.voto().traza(), "Envía la entrada #" + e.indice()
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
                            if (e.tipo() == EntradaLog.Tipo.VOTO) {
                                ev("REPLICACION", "ACK", e.voto().traza(), "El nodo " + par.id()
                                        + " confirma que guardó la entrada #" + e.indice() + " en su log",
                                        Evento.datos("seguidor", par.id()));
                            }
                        }
                        indiceCoincidente.merge(par.id(), r.indiceCoincidente(), Math::max);
                        siguienteIndice.put(par.id(), indiceCoincidente.get(par.id()) + 1);
                        avanzarConfirmacion();
                    } else {
                        // Inconsistencia: retroceder hasta encontrar el punto en común con el seguidor.
                        siguienteIndice.put(par.id(), Math.max(1, Math.min(r.indiceConflicto(), previo)));
                        ev("REPLICACION", "INCONSISTENCIA", null, "El log del nodo " + par.id()
                                + " no coincide en el índice " + previo + ": se reintentará desde el "
                                + siguienteIndice.get(par.id()), null);
                    }
                    pendiente = siguienteIndice.get(par.id()) <= ultimoIndice();
                }
            } catch (Exception e) {
                // par caído o pausado: se reintenta en el próximo latido
                if (paresInaccesibles.add(par.id())) {
                    ev("RED", "PAR_CAIDO", null, "El nodo " + par.id() + " no responde a las llamadas RMI (caído o pausado)",
                            Evento.datos("error", e.getClass().getSimpleName()));
                }
            } finally {
                enVuelo.remove(par.id());
            }
            if (pendiente) {
                synchronized (this) {
                    if (rol == Rol.LIDER && !pausado) replicarA(par); // ponerlo al día sin esperar el latido
                }
            }
        });
    }

    /** El líder confirma la entrada N si está replicada en la mayoría y es de su término. */
    private void avanzarConfirmacion() {
        for (int n = ultimoIndice(); n > indiceConfirmado; n--) {
            if (registro.get(n).termino() != terminoActual) break;
            final int indice = n;
            long replicas = 1 + pares.keySet().stream()
                    .filter(p -> indiceCoincidente.getOrDefault(p, 0) >= indice).count();
            if (replicas >= mayoria()) {
                for (int k = indiceConfirmado + 1; k <= n; k++) {
                    EntradaLog e = registro.get(k);
                    if (e.tipo() == EntradaLog.Tipo.VOTO) {
                        ev("VOTO", "CONFIRMADO", e.voto().traza(), "Entrada #" + k + " CONFIRMADA: ya está en "
                                + replicas + " de " + tamanoCluster() + " nodos (mayoría = " + mayoria() + ")", null);
                    }
                }
                indiceConfirmado = n;
                aplicarConfirmadas();
                break;
            }
        }
    }

    /** RPC AppendEntries, lado receptor. También funciona como latido del líder. */
    public synchronized RespuestaAnexar alAnexarEntradas(SolicitudAnexar s) {
        if (s.termino() < terminoActual) return new RespuestaAnexar(terminoActual, false, 0, 0);

        if (s.termino() > terminoActual || rol != Rol.SEGUIDOR) retrocederASeguidor(s.termino(), s.liderId());
        if (liderId == null || liderId != s.liderId()) {
            ev("ELECCION", "RECONOCE_LIDER", null, "Reconoce al nodo " + s.liderId() + " como LÍDER del término " + s.termino(), null);
        }
        liderId = s.liderId();
        reiniciarTemporizadorEleccion();

        // Verificación de consistencia del log
        if (s.indiceLogPrevio() > ultimoIndice()) {
            return new RespuestaAnexar(terminoActual, false, 0, ultimoIndice() + 1);
        }
        if (registro.get(s.indiceLogPrevio()).termino() != s.terminoLogPrevio()) {
            // Saltar todo el término en conflicto para retroceder más rápido.
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
                if (entrada.tipo() == EntradaLog.Tipo.VOTO) {
                    ev("REPLICACION", "ENTRADA_RECIBIDA", entrada.voto().traza(), "Recibe del líder (nodo "
                            + s.liderId() + ") la entrada #" + i + " y la agrega a su log (aún sin confirmar)", null);
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
            boolean aplicado = maquina.aplicar(entrada);
            if (entrada.tipo() == EntradaLog.Tipo.VOTO) {
                String traza = entrada.voto().traza();
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

    // ================================================================== escritura de votos

    /**
     * Registra un voto. Solo el líder acepta escrituras: agrega la entrada al log,
     * la replica y espera a que la mayoría la confirme.
     */
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
            EntradaLog entrada = anexar(EntradaLog.Tipo.VOTO, voto);
            indice = entrada.indice();
            esperas.put(indice, new Espera(entrada.id(), futuro));
            ev("VOTO", "ANEXADO_LOG", traza, "Agrega el voto a su log como entrada #" + indice
                    + " (término " + terminoActual + ") y lo replica a los seguidores",
                    Evento.datos("indice", indice, "candidato", candidato));
            avanzarConfirmacion(); // en un clúster de 1 nodo se confirma de inmediato
            replicarATodos();
        }
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
                    "No se logró replicar el voto en la mayoría del clúster a tiempo");
        } catch (ExecutionException e) {
            if (e.getCause() instanceof VotoRechazadoException v) throw v;
            throw new IllegalStateException(e.getCause());
        }
    }

    // ================================================================== trazabilidad

    /** Emite un evento con el rol y término actuales del nodo. */
    synchronized void ev(String categoria, String etapa, String traza, String mensaje, java.util.Map<String, String> datos) {
        bitacora.publicar(Evento.nuevo("NODO", id, rol.name(), terminoActual, categoria, etapa, traza, mensaje, datos));
    }

    void suscribir(OyenteEventos oyente) {
        if (bitacora.suscribir(oyente)) {
            ev("SISTEMA", "SUSCRIPCION", null, "El gateway se suscribió a los eventos de este nodo (callback RMI)", null);
        }
    }

    ArchivoNodo leerArchivo() {
        return almacen.leer(200);
    }

    // ================================================================== utilidades

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

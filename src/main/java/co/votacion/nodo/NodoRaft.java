package co.votacion.nodo;

import co.votacion.comun.ClienteRmi;
import co.votacion.comun.EntradaLog;
import co.votacion.comun.EstadoNodo;
import co.votacion.comun.RespuestaAnexar;
import co.votacion.comun.RespuestaVoto;
import co.votacion.comun.SolicitudAnexar;
import co.votacion.comun.SolicitudVoto;
import co.votacion.comun.Voto;
import co.votacion.comun.VotoRechazadoException;
import co.votacion.comun.VotoRechazadoException.Codigo;

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
import java.util.function.Consumer;

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
    private final Consumer<String> log;

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

    public NodoRaft(int id, List<ClienteRmi> pares, ConfigRaft cfg, Consumer<String> log) {
        this.id = id;
        pares.forEach(p -> this.pares.put(p.id(), p));
        this.cfg = cfg;
        this.log = log;
        this.planificador = Executors.newSingleThreadScheduledExecutor(hilo("raft-" + id + "-timer"));
        this.rpcs = Executors.newCachedThreadPool(hilo("raft-" + id + "-rpc"));
    }

    // ================================================================== ciclo de vida

    public synchronized void iniciar() {
        log.accept("Nodo " + id + " iniciado como SEGUIDOR (cluster de " + tamanoCluster() + " nodos)");
        reiniciarTemporizadorEleccion();
    }

    public synchronized void detener() {
        cancelar(temporizadorEleccion);
        cancelar(temporizadorLatido);
        planificador.shutdownNow();
        rpcs.shutdownNow();
    }

    /** Simula una caída / partición de red: el nodo deja de participar. */
    public synchronized void pausar() {
        pausado = true;
        cancelar(temporizadorEleccion);
        cancelar(temporizadorLatido);
        log.accept("Nodo " + id + " PAUSADO (simulando caida)");
    }

    public synchronized void reanudar() {
        if (!pausado) return;
        pausado = false;
        // Si era líder, sigue enviando latidos; si ya hay otro líder con término mayor,
        // la primera respuesta lo hará retroceder a seguidor.
        if (rol == Rol.LIDER) iniciarLatidos();
        else reiniciarTemporizadorEleccion();
        log.accept("Nodo " + id + " REANUDADO");
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
        log.accept("Nodo " + id + " inicia ELECCION para el termino " + termino);
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
                        if (r.votoConcedido() && votos.incrementAndGet() >= mayoria()) convertirseEnLider();
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
        log.accept("*** Nodo " + id + " es LIDER en el termino " + terminoActual + " ***");
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
        if (rol != Rol.SEGUIDOR) log.accept("Nodo " + id + " pasa a SEGUIDOR (termino " + terminoActual + ")");
        rol = Rol.SEGUIDOR;
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
            log.accept("Nodo " + id + " vota por el nodo " + s.candidatoId() + " (termino " + s.termino() + ")");
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

        rpcs.submit(() -> {
            boolean pendiente = false;
            try {
                RespuestaAnexar r = par.llamar(n -> n.anexarEntradas(solicitud));
                synchronized (this) {
                    if (pausado) return;
                    if (r.termino() > terminoActual) {
                        retrocederASeguidor(r.termino(), null);
                        return;
                    }
                    if (rol != Rol.LIDER || terminoActual != termino) return;
                    if (r.exito()) {
                        indiceCoincidente.merge(par.id(), r.indiceCoincidente(), Math::max);
                        siguienteIndice.put(par.id(), indiceCoincidente.get(par.id()) + 1);
                        avanzarConfirmacion();
                    } else {
                        // Inconsistencia: retroceder hasta encontrar el punto en común con el seguidor.
                        siguienteIndice.put(par.id(), Math.max(1, Math.min(r.indiceConflicto(), previo)));
                    }
                    pendiente = siguienteIndice.get(par.id()) <= ultimoIndice();
                }
            } catch (Exception e) {
                // par caído o pausado: se reintenta en el próximo latido
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
            log.accept("Nodo " + id + " reconoce al nodo " + s.liderId() + " como LIDER");
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
            if (i > ultimoIndice()) registro.add(entrada);
        }

        int ultimoNuevo = s.indiceLogPrevio() + s.entradas().size();
        if (s.confirmadoLider() > indiceConfirmado) {
            indiceConfirmado = Math.min(s.confirmadoLider(), ultimoNuevo);
            aplicarConfirmadas();
        }
        return new RespuestaAnexar(terminoActual, true, ultimoNuevo, 0);
    }

    private void truncarDesde(int indice) {
        log.accept("Nodo " + id + " descarta entradas NO confirmadas desde el indice " + indice);
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
                String cedula = entrada.voto().cedula();
                log.accept("Nodo " + id + " aplica voto #" + entrada.indice() + " (cedula ..."
                        + cedula.substring(cedula.length() - 4) + ")" + (aplicado ? "" : " RECHAZADO: duplicado"));
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
    EntradaLog registrarComoLider(String nombre, String cedula, String candidato)
            throws NoEsLiderException, VotoRechazadoException, InterruptedException {
        CompletableFuture<EntradaLog> futuro = new CompletableFuture<>();
        int indice;
        synchronized (this) {
            if (rol != Rol.LIDER || pausado) throw new NoEsLiderException(liderId);

            boolean pendienteMismaCedula = registro.subList(indiceConfirmado + 1, registro.size()).stream()
                    .anyMatch(e -> e.tipo() == EntradaLog.Tipo.VOTO && e.voto().cedula().equals(cedula));
            if (maquina.yaVoto(cedula) || pendienteMismaCedula) {
                throw new VotoRechazadoException(Codigo.CEDULA_DUPLICADA, "Esta cédula ya registró su voto");
            }

            Voto voto = new Voto(nombre, cedula, candidato, java.time.Instant.now().toString(), id);
            EntradaLog entrada = anexar(EntradaLog.Tipo.VOTO, voto);
            indice = entrada.indice();
            esperas.put(indice, new Espera(entrada.id(), futuro));
            log.accept("Nodo " + id + " (lider) agrega voto #" + indice + " al log y lo replica");
            avanzarConfirmacion(); // en un clúster de 1 nodo se confirma de inmediato
            replicarATodos();
        }
        try {
            return futuro.get(cfg.timeoutConfirmacionMs(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            synchronized (this) {
                esperas.remove(indice);
            }
            throw new VotoRechazadoException(Codigo.TIMEOUT,
                    "No se logró replicar el voto en la mayoría del clúster a tiempo");
        } catch (ExecutionException e) {
            if (e.getCause() instanceof VotoRechazadoException v) throw v;
            throw new IllegalStateException(e.getCause());
        }
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

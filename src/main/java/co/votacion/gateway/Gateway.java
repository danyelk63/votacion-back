package co.votacion.gateway;

import co.votacion.comun.ArchivoNodo;
import co.votacion.comun.Candidato;
import co.votacion.comun.ClienteRmi;
import co.votacion.comun.DireccionNodo;
import co.votacion.comun.EstadoNodo;
import co.votacion.comun.Evento;
import co.votacion.comun.ResultadoBorrado;
import co.votacion.comun.ResultadoRegistro;
import co.votacion.comun.SolicitudBorrado;
import co.votacion.comun.SolicitudRegistro;
import co.votacion.comun.VotoRechazadoException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * API Gateway: puente entre el navegador y el clúster.
 *
 * <p>El navegador no puede hacer llamadas RMI, así que:
 * <ul>
 *   <li>Recibe <b>HTTP/REST</b> del frontend y actúa como <b>cliente RMI</b> de los nodos
 *       (balanceo round-robin + failover).</li>
 *   <li>Exporta un objeto de <b>callback RMI</b> ({@link ReceptorEventos}) al que los nodos
 *       le envían cada evento que generan.</li>
 *   <li>Reenvía todos esos eventos al navegador en tiempo real por <b>WebSocket</b>.</li>
 *   <li>Lleva el log de auditoría de usuarios en {@code datos/gateway/usuarios.log}.</li>
 * </ul>
 *
 * <pre>
 * GET  /api/candidatos
 * GET  /api/resultados
 * GET  /api/votos
 * POST /api/votos                  {"nombre","cedula","candidato"}
 * DELETE /api/votos                borra todos los votos (por consenso en todo el clúster)
 * GET  /api/cluster
 * POST /api/cluster/{id}/pausar
 * POST /api/cluster/{id}/reanudar
 * GET  /api/eventos                historial de eventos (también llega por WebSocket)
 * GET  /api/archivos               contenido de votos.txt de cada nodo y de usuarios.log
 * WS   ws://host:puertoWs          eventos en tiempo real
 * </pre>
 */
public class Gateway {

    private static final Pattern RUTA_ADMIN = Pattern.compile("^/api/cluster/(\\d+)/(pausar|reanudar)$");

    private final int puerto;
    private final int puertoWs;
    private final int puertoCallback;
    private final List<ClienteRmi> nodos;
    private final Path dirDatos;
    private final boolean silencioso;
    private final ObjectMapper json = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    private final AtomicInteger turno = new AtomicInteger();

    private CentralEventos central;
    private BitacoraUsuarios usuarios;
    private ReceptorEventos receptor;
    private ServidorWebSocket webSocket;
    private HttpServer servidor;
    private ExecutorService hilosHttp;
    private ScheduledExecutorService suscripciones;

    /** Respuesta HTTP a devolver. */
    private record Respuesta(int status, Object cuerpo) {
    }

    /** Llamada RMI a un nodo concreto. */
    @FunctionalInterface
    private interface Operacion {
        Object ejecutar(ClienteRmi nodo) throws Exception;
    }

    /** Resultado de una operación con failover: respuesta + nodo que la atendió. */
    private record ResultadoFailover(Respuesta respuesta, Integer nodo, VotoRechazadoException rechazo) {
    }

    public Gateway(int puerto, int puertoWs, int puertoCallback, List<DireccionNodo> nodos, Path dirDatos,
                   boolean silencioso) {
        this.puerto = puerto;
        this.puertoWs = puertoWs;
        this.puertoCallback = puertoCallback;
        this.nodos = nodos.stream().map(ClienteRmi::new).toList();
        this.dirDatos = dirDatos;
        this.silencioso = silencioso;
    }

    public void iniciar() throws IOException {
        central = new CentralEventos(dirDatos.resolve("eventos.log"), !silencioso);
        usuarios = new BitacoraUsuarios(dirDatos.resolve("usuarios.log"), central);

        // 1. Objeto remoto de callback para que los nodos nos envíen sus eventos.
        receptor = new ReceptorEventos(central, puertoCallback);
        central.gateway("SISTEMA", "CALLBACK_RMI", null, "Objeto remoto de callback exportado: los nodos invocarán recibir() en él",
                Evento.datos("puerto", puertoCallback));

        // 2. WebSocket hacia el navegador.
        webSocket = new ServidorWebSocket(puertoWs, central, json, this::log);
        webSocket.start();

        // 3. API REST.
        servidor = HttpServer.create(new InetSocketAddress(puerto), 0);
        hilosHttp = Executors.newFixedThreadPool(16);
        servidor.setExecutor(hilosHttp);
        servidor.createContext("/", this::atender);
        servidor.start();
        central.gateway("SISTEMA", "INICIO", null, "Gateway escuchando HTTP en el puerto " + puerto
                + " y WebSocket en el " + puertoWs, Evento.datos("nodos", nodos.stream().map(n -> n.direccion().toString()).toList()));

        // 4. Suscribirse a cada nodo (y re-suscribirse si un nodo se reinicia).
        suscripciones = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "gateway-suscripciones");
            t.setDaemon(true);
            return t;
        });
        suscripciones.scheduleWithFixedDelay(this::suscribirANodos, 0, 2, TimeUnit.SECONDS);
    }

    private void suscribirANodos() {
        for (ClienteRmi n : nodos) {
            try {
                n.llamar(s -> {
                    s.suscribir(receptor);
                    return null;
                });
            } catch (Exception ignorada) {
                // nodo caído: se reintenta en 2 s
            }
        }
    }

    public CentralEventos central() {
        return central;
    }

    public void detener() {
        if (suscripciones != null) suscripciones.shutdownNow();
        if (servidor != null) servidor.stop(0);
        if (hilosHttp != null) hilosHttp.shutdownNow();
        if (webSocket != null) webSocket.detener();
        try {
            UnicastRemoteObject.unexportObject(receptor, true);
        } catch (Exception ignorada) {
        }
    }

    // ================================================================== enrutamiento

    private void atender(HttpExchange ex) throws IOException {
        try (ex) {
            agregarCors(ex);
            String metodo = ex.getRequestMethod();
            String ruta = ex.getRequestURI().getPath().replaceAll("/+$", "");
            if (metodo.equals("OPTIONS")) {
                ex.sendResponseHeaders(204, -1);
                return;
            }

            Respuesta r;
            Matcher admin = RUTA_ADMIN.matcher(ruta);
            if (metodo.equals("GET") && ruta.equals("/api/candidatos")) {
                r = new Respuesta(200, Candidato.TODOS);
            } else if (metodo.equals("GET") && ruta.equals("/api/resultados")) {
                r = conFailover(n -> n.llamar(s -> s.resultados(false)), null).respuesta();
            } else if (metodo.equals("GET") && ruta.equals("/api/votos")) {
                r = conFailover(n -> n.llamar(s -> s.votos(false)), null).respuesta();
            } else if (metodo.equals("POST") && ruta.equals("/api/votos")) {
                r = registrarVoto(ex);
            } else if (metodo.equals("DELETE") && ruta.equals("/api/votos")) {
                r = borrarVotos(ex);
            } else if (metodo.equals("GET") && ruta.equals("/api/cluster")) {
                r = estadoCluster();
            } else if (metodo.equals("POST") && admin.matches()) {
                r = administrar(ex, Integer.parseInt(admin.group(1)), admin.group(2));
            } else if (metodo.equals("GET") && ruta.equals("/api/eventos")) {
                r = new Respuesta(200, central.historial());
            } else if (metodo.equals("GET") && ruta.equals("/api/archivos")) {
                r = archivos();
            } else if (metodo.equals("GET") && ruta.equals("/salud")) {
                r = new Respuesta(200, Map.of("ok", true));
            } else {
                r = new Respuesta(404, Map.of("error", "Ruta no encontrada"));
            }
            enviar(ex, r);
        } catch (Exception e) {
            e.printStackTrace();
            enviar(ex, new Respuesta(500, Map.of("error", "Error interno del gateway")));
        }
    }

    // ================================================================== votos

    private Respuesta registrarVoto(HttpExchange ex) throws IOException {
        long inicio = System.nanoTime();
        String traza = UUID.randomUUID().toString().substring(0, 8);
        String ip = ex.getRemoteAddress().getAddress().getHostAddress();
        String reenviadoDe = ex.getRequestHeaders().getFirst("X-Forwarded-For");
        if (reenviadoDe != null) ip = reenviadoDe.split(",")[0].trim();
        String navegador = navegador(ex.getRequestHeaders().getFirst("User-Agent"));

        Map<?, ?> cuerpo;
        try {
            cuerpo = json.readValue(ex.getRequestBody(), Map.class);
        } catch (Exception e) {
            return new Respuesta(400, Map.of("error", "JSON inválido"));
        }
        SolicitudRegistro solicitud = new SolicitudRegistro(texto(cuerpo.get("nombre")), texto(cuerpo.get("cedula")),
                texto(cuerpo.get("candidato")), null, traza);

        central.gateway("VOTO", "RECIBIDO_HTTP", traza, "Llega POST /api/votos desde el navegador (HTTP/JSON). Se asigna la traza " + traza,
                Evento.datos("accion", "VOTAR", "ip", ip, "navegador", navegador, "nombre", solicitud.nombre(),
                        "cedula", Evento.enmascarar(solicitud.cedula()), "candidato", solicitud.candidato()));

        ResultadoFailover res = conFailover(n -> {
            central.gateway("RED", "LLAMADA_RMI", traza, "Invoca registrarVoto() en el nodo " + n.id() + " por RMI",
                    Evento.datos("destino", n.direccion()));
            return n.llamar(s -> s.registrarVoto(solicitud));
        }, traza);

        long ms = (System.nanoTime() - inicio) / 1_000_000;
        Respuesta r = res.respuesta();
        central.gateway("VOTO", "RESPUESTA_HTTP", traza, "Responde HTTP " + r.status() + " al navegador",
                Evento.datos("duracionMs", ms, "nodoAtendio", res.nodo()));

        String resultado;
        String detalle;
        if (r.status() == 200) {
            ResultadoRegistro rr = json.convertValue(r.cuerpo(), ResultadoRegistro.class);
            resultado = "ACEPTADO";
            detalle = "entrada #" + rr.indiceLog() + " confirmada por el líder nodo " + rr.lider() + " en " + ms + " ms";
        } else if (res.rechazo() != null) {
            resultado = "RECHAZADO_" + res.rechazo().getCodigo().name();
            detalle = res.rechazo().getMessage() + (res.rechazo().getDetalles().isEmpty() ? "" : ": " + String.join("; ", res.rechazo().getDetalles()));
        } else {
            resultado = "ERROR_" + r.status();
            detalle = "clúster no disponible";
        }
        usuarios.registrar(new BitacoraUsuarios.Accion("VOTAR", resultado, solicitud.nombre(), solicitud.cedula(),
                solicitud.candidato(), ip, navegador, res.nodo(), traza, detalle));

        if (r.cuerpo() instanceof Map<?, ?> m) {
            Map<String, Object> conTraza = new LinkedHashMap<>();
            m.forEach((k, v) -> conTraza.put(String.valueOf(k), v));
            conTraza.put("traza", traza);
            conTraza.put("duracionMs", ms);
            return new Respuesta(r.status(), conTraza);
        }
        return r;
    }

    /** Borra todos los votos: la orden se replica por Raft a todos los nodos. */
    private Respuesta borrarVotos(HttpExchange ex) {
        long inicio = System.nanoTime();
        String traza = UUID.randomUUID().toString().substring(0, 8);
        String ip = ex.getRemoteAddress().getAddress().getHostAddress();
        String navegador = navegador(ex.getRequestHeaders().getFirst("User-Agent"));
        SolicitudBorrado solicitud = new SolicitudBorrado(null, traza);

        central.gateway("VOTO", "RECIBIDO_HTTP", traza, "Llega DELETE /api/votos desde el navegador: orden de borrar todos los votos. Se asigna la traza " + traza,
                Evento.datos("accion", "BORRAR_VOTOS", "ip", ip, "navegador", navegador));
        ResultadoFailover res = conFailover(n -> {
            central.gateway("RED", "LLAMADA_RMI", traza, "Invoca borrarVotos() en el nodo " + n.id() + " por RMI",
                    Evento.datos("destino", n.direccion()));
            return n.llamar(s -> s.borrarVotos(solicitud));
        }, traza);
        long ms = (System.nanoTime() - inicio) / 1_000_000;
        Respuesta r = res.respuesta();
        central.gateway("VOTO", "RESPUESTA_HTTP", traza, "Responde HTTP " + r.status() + " al navegador",
                Evento.datos("duracionMs", ms, "nodoAtendio", res.nodo()));

        String resultado;
        String detalle;
        if (r.status() == 200) {
            ResultadoBorrado rb = json.convertValue(r.cuerpo(), ResultadoBorrado.class);
            resultado = "OK";
            detalle = rb.votosBorrados() + " voto(s) borrados con la orden #" + rb.indiceLog() + " del líder nodo " + rb.lider();
        } else {
            resultado = res.rechazo() != null ? "RECHAZADO_" + res.rechazo().getCodigo().name() : "ERROR_" + r.status();
            detalle = res.rechazo() != null ? res.rechazo().getMessage() : "clúster no disponible";
        }
        usuarios.registrar(new BitacoraUsuarios.Accion("BORRAR_VOTOS", resultado, null, null, null, ip, navegador,
                res.nodo(), traza, detalle));

        if (r.cuerpo() instanceof Map<?, ?> m) {
            Map<String, Object> conTraza = new LinkedHashMap<>();
            m.forEach((k, v) -> conTraza.put(String.valueOf(k), v));
            conTraza.put("traza", traza);
            conTraza.put("duracionMs", ms);
            return new Respuesta(r.status(), conTraza);
        }
        return r;
    }

    /**
     * Ejecuta la operación en los nodos, en orden round-robin, hasta obtener una
     * respuesta definitiva. Un nodo caído (RemoteException) o sin líder => siguiente nodo.
     */
    private ResultadoFailover conFailover(Operacion op, String traza) {
        int inicio = Math.floorMod(turno.getAndIncrement(), nodos.size());
        List<String> intentos = new ArrayList<>();
        for (int ronda = 0; ronda < 3; ronda++) {
            for (int k = 0; k < nodos.size(); k++) {
                ClienteRmi nodo = nodos.get((inicio + k) % nodos.size());
                try {
                    Object resultado = op.ejecutar(nodo);
                    return new ResultadoFailover(new Respuesta(200, conNodo(resultado, nodo.id())), nodo.id(), null);
                } catch (VotoRechazadoException e) {
                    if (!e.getCodigo().reintentable) {
                        return new ResultadoFailover(respuestaRechazo(e, nodo.id()), nodo.id(), e);
                    }
                    intentos.add("nodo " + nodo.id() + ": " + e.getMessage());
                    if (traza != null) central.gateway("RED", "FAILOVER", traza, "El nodo " + nodo.id()
                            + " no pudo atender (" + e.getMessage() + "): prueba con el siguiente nodo", null);
                } catch (Exception e) {
                    intentos.add("nodo " + nodo.id() + ": no responde");
                    if (traza != null) central.gateway("RED", "FAILOVER", traza, "El nodo " + nodo.id()
                            + " no responde por RMI (" + e.getClass().getSimpleName() + "): prueba con el siguiente nodo", null);
                }
            }
            // Probablemente hay una elección de líder en curso: esperar antes de otra ronda.
            if (ronda < 2) dormir(1000);
        }
        return new ResultadoFailover(new Respuesta(503, Map.of("error", "El clúster no está disponible en este momento",
                "intentos", intentos)), null, null);
    }

    private Respuesta respuestaRechazo(VotoRechazadoException e, int nodoId) {
        int status = switch (e.getCodigo()) {
            case DATOS_INVALIDOS -> 400;
            case CEDULA_DUPLICADA -> 409;
            default -> 503;
        };
        Map<String, Object> cuerpo = new LinkedHashMap<>();
        cuerpo.put("error", e.getMessage());
        cuerpo.put("codigo", e.getCodigo().name());
        if (!e.getDetalles().isEmpty()) cuerpo.put("detalles", e.getDetalles());
        cuerpo.put("atendidoPorNodo", nodoId);
        return new Respuesta(status, cuerpo);
    }

    // ================================================================== clúster y archivos

    /** Consulta en paralelo el estado de todos los nodos (panel de monitoreo). */
    private Respuesta estadoCluster() {
        List<CompletableFuture<Map<String, Object>>> consultas = nodos.stream()
                .map(n -> CompletableFuture.supplyAsync(() -> {
                    Map<String, Object> m = new LinkedHashMap<>();
                    m.put("direccion", n.direccion().toString());
                    try {
                        EstadoNodo e = n.llamar(s -> s.estado());
                        m.putAll(json.convertValue(e, Map.class));
                        m.put("disponible", !e.pausado());
                    } catch (Exception e) {
                        m.put("id", n.id());
                        m.put("disponible", false);
                        m.put("pausado", false);
                        m.put("rol", "CAIDO");
                    }
                    return m;
                })).toList();
        List<Map<String, Object>> estados = consultas.stream().map(CompletableFuture::join).toList();
        Object liderActual = estados.stream()
                .filter(e -> Boolean.TRUE.equals(e.get("disponible")) && "LIDER".equals(e.get("rol")))
                .max(Comparator.comparingLong(e -> ((Number) e.get("termino")).longValue()))
                .map(e -> e.get("id")).orElse(null);
        Map<String, Object> cuerpo = new LinkedHashMap<>();
        cuerpo.put("liderActual", liderActual);
        cuerpo.put("nodos", estados);
        cuerpo.put("clientesWebSocket", webSocket.clientes());
        return new Respuesta(200, cuerpo);
    }

    /** Lee por RMI el archivo votos.txt de cada nodo, más el usuarios.log del gateway. */
    private Respuesta archivos() {
        List<CompletableFuture<Map<String, Object>>> lecturas = nodos.stream()
                .map(n -> CompletableFuture.supplyAsync(() -> {
                    Map<String, Object> m = new LinkedHashMap<>();
                    try {
                        ArchivoNodo a = n.llamar(s -> s.leerArchivo());
                        m.putAll(json.convertValue(a, Map.class));
                        m.put("disponible", true);
                    } catch (Exception e) {
                        m.put("nodoId", n.id());
                        m.put("disponible", false);
                    }
                    return m;
                })).toList();
        Map<String, Object> cuerpo = new LinkedHashMap<>();
        cuerpo.put("nodos", lecturas.stream().map(CompletableFuture::join).toList());
        cuerpo.put("usuarios", Map.of("ruta", usuarios.archivo().toAbsolutePath().toString(),
                "totalLineas", usuarios.totalLineas(), "lineas", usuarios.leer(200)));
        return new Respuesta(200, cuerpo);
    }

    private Respuesta administrar(HttpExchange ex, int id, String accion) {
        ClienteRmi nodo = nodos.stream().filter(n -> n.id() == id).findFirst().orElse(null);
        if (nodo == null) return new Respuesta(404, Map.of("error", "Nodo no encontrado"));
        String ip = ex.getRemoteAddress().getAddress().getHostAddress();
        String nav = navegador(ex.getRequestHeaders().getFirst("User-Agent"));
        String nombreAccion = accion.equals("pausar") ? "TUMBAR_NODO" : "REANUDAR_NODO";
        try {
            EstadoNodo e = nodo.llamar(s -> accion.equals("pausar") ? s.pausar() : s.reanudar());
            usuarios.registrar(new BitacoraUsuarios.Accion(nombreAccion, "OK", null, null, null, ip, nav, id, null,
                    "simulación de fallo desde el panel"));
            return new Respuesta(200, e);
        } catch (Exception e) {
            usuarios.registrar(new BitacoraUsuarios.Accion(nombreAccion, "ERROR", null, null, null, ip, nav, id, null,
                    "el proceso del nodo no responde"));
            return new Respuesta(502, Map.of("error", "El proceso del nodo " + id + " no responde"));
        }
    }

    // ================================================================== utilidades

    @SuppressWarnings("unchecked")
    private Object conNodo(Object resultado, int nodoId) {
        Map<String, Object> m = new LinkedHashMap<>(json.convertValue(resultado, Map.class));
        m.put("atendidoPorNodo", nodoId);
        return m;
    }

    private void enviar(HttpExchange ex, Respuesta r) throws IOException {
        byte[] bytes = json.writeValueAsBytes(r.cuerpo());
        ex.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        ex.sendResponseHeaders(r.status(), bytes.length);
        try (OutputStream os = ex.getResponseBody()) {
            os.write(bytes);
        }
    }

    private static void agregarCors(HttpExchange ex) {
        ex.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
        ex.getResponseHeaders().set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS");
        ex.getResponseHeaders().set("Access-Control-Allow-Headers", "Content-Type");
    }

    /** Resume el User-Agent a algo legible ("Chrome", "Firefox"...). */
    private static String navegador(String ua) {
        if (ua == null) return "desconocido";
        if (ua.contains("Edg/")) return "Edge";
        if (ua.contains("OPR/")) return "Opera";
        if (ua.contains("Firefox/")) return "Firefox";
        if (ua.contains("Chrome/")) return "Chrome";
        if (ua.contains("Safari/")) return "Safari";
        if (ua.startsWith("Java-http-client")) return "Cliente Java";
        if (ua.startsWith("curl")) return "curl";
        return ua.length() > 40 ? ua.substring(0, 40) : ua;
    }

    private static String texto(Object o) {
        return o == null ? null : o.toString();
    }

    private static void dormir(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void log(String m) {
        if (!silencioso) System.out.println("[gateway] " + m);
    }
}

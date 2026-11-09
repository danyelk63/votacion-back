package co.votacion.gateway;

import co.votacion.comun.Candidato;
import co.votacion.comun.ClienteRmi;
import co.votacion.comun.DireccionNodo;
import co.votacion.comun.EstadoNodo;
import co.votacion.comun.SolicitudRegistro;
import co.votacion.comun.VotoRechazadoException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * API Gateway: puente entre el navegador (HTTP/JSON) y el clúster (Java RMI).
 *
 * <p>El navegador no puede hacer llamadas RMI, así que el frontend Angular habla
 * REST con este proceso, y este actúa como <b>cliente RMI</b> de los nodos:
 * <ul>
 *   <li>Balancea las peticiones entre los nodos (round-robin).</li>
 *   <li>Failover: si un nodo no responde o no hay líder, reintenta con el siguiente.</li>
 *   <li>Expone el estado del clúster y permite simular caídas de nodos.</li>
 * </ul>
 *
 * <pre>
 * GET  /api/candidatos
 * GET  /api/resultados
 * GET  /api/votos
 * POST /api/votos                  {"nombre","cedula","candidato"}
 * GET  /api/cluster
 * POST /api/cluster/{id}/pausar
 * POST /api/cluster/{id}/reanudar
 * </pre>
 */
public class Gateway {

    private static final Pattern RUTA_ADMIN = Pattern.compile("^/api/cluster/(\\d+)/(pausar|reanudar)$");

    private final int puerto;
    private final List<ClienteRmi> nodos;
    private final boolean silencioso;
    private final ObjectMapper json = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    private final AtomicInteger turno = new AtomicInteger();
    private HttpServer servidor;

    /** Respuesta HTTP a devolver. */
    private record Respuesta(int status, Object cuerpo) {
    }

    /** Llamada RMI a un nodo concreto. */
    @FunctionalInterface
    private interface Operacion {
        Object ejecutar(ClienteRmi nodo) throws Exception;
    }

    public Gateway(int puerto, List<DireccionNodo> nodos, boolean silencioso) {
        this.puerto = puerto;
        this.nodos = nodos.stream().map(ClienteRmi::new).toList();
        this.silencioso = silencioso;
    }

    public void iniciar() throws IOException {
        servidor = HttpServer.create(new InetSocketAddress(puerto), 0);
        servidor.setExecutor(Executors.newFixedThreadPool(16));
        servidor.createContext("/", this::atender);
        servidor.start();
        log("escuchando en http://localhost:" + puerto + " -> nodos " + nodos.stream()
                .map(n -> n.direccion().toString()).toList());
    }

    public void detener() {
        if (servidor != null) servidor.stop(0);
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
                r = conFailover(n -> n.llamar(s -> s.resultados(false)));
            } else if (metodo.equals("GET") && ruta.equals("/api/votos")) {
                r = conFailover(n -> n.llamar(s -> s.votos(false)));
            } else if (metodo.equals("POST") && ruta.equals("/api/votos")) {
                r = registrarVoto(ex);
            } else if (metodo.equals("GET") && ruta.equals("/api/cluster")) {
                r = estadoCluster();
            } else if (metodo.equals("POST") && admin.matches()) {
                r = administrar(Integer.parseInt(admin.group(1)), admin.group(2));
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

    // ================================================================== operaciones

    private Respuesta registrarVoto(HttpExchange ex) throws IOException {
        Map<?, ?> cuerpo;
        try {
            cuerpo = json.readValue(ex.getRequestBody(), Map.class);
        } catch (Exception e) {
            return new Respuesta(400, Map.of("error", "JSON inválido"));
        }
        SolicitudRegistro solicitud = new SolicitudRegistro(texto(cuerpo.get("nombre")),
                texto(cuerpo.get("cedula")), texto(cuerpo.get("candidato")), null);
        return conFailover(n -> n.llamar(s -> s.registrarVoto(solicitud)));
    }

    /**
     * Ejecuta la operación en los nodos, en orden round-robin, hasta obtener una
     * respuesta definitiva. Un nodo caído (RemoteException) o sin líder => siguiente nodo.
     */
    private Respuesta conFailover(Operacion op) {
        int inicio = Math.floorMod(turno.getAndIncrement(), nodos.size());
        List<String> intentos = new ArrayList<>();
        for (int ronda = 0; ronda < 3; ronda++) {
            for (int k = 0; k < nodos.size(); k++) {
                ClienteRmi nodo = nodos.get((inicio + k) % nodos.size());
                try {
                    Object resultado = op.ejecutar(nodo);
                    return new Respuesta(200, conNodo(resultado, nodo.id()));
                } catch (VotoRechazadoException e) {
                    if (!e.getCodigo().reintentable) return respuestaRechazo(e, nodo.id());
                    intentos.add("nodo " + nodo.id() + ": " + e.getMessage());
                } catch (Exception e) {
                    intentos.add("nodo " + nodo.id() + ": no responde");
                }
            }
            // Probablemente hay una elección de líder en curso: esperar antes de otra ronda.
            if (ronda < 2) dormir(1000);
        }
        log("ningun nodo pudo atender la peticion: " + intentos);
        return new Respuesta(503, Map.of("error", "El clúster no está disponible en este momento",
                "intentos", intentos));
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
        return new Respuesta(200, cuerpo);
    }

    private Respuesta administrar(int id, String accion) {
        ClienteRmi nodo = nodos.stream().filter(n -> n.id() == id).findFirst().orElse(null);
        if (nodo == null) return new Respuesta(404, Map.of("error", "Nodo no encontrado"));
        try {
            EstadoNodo e = nodo.llamar(s -> accion.equals("pausar") ? s.pausar() : s.reanudar());
            log("nodo " + id + ": " + accion);
            return new Respuesta(200, e);
        } catch (Exception e) {
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
        ex.getResponseHeaders().set("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
        ex.getResponseHeaders().set("Access-Control-Allow-Headers", "Content-Type");
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

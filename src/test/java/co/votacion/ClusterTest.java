package co.votacion;

import co.votacion.comun.ClienteRmi;
import co.votacion.comun.DireccionNodo;
import co.votacion.comun.SolicitudRegistro;
import co.votacion.comun.VotoRechazadoException;
import co.votacion.gateway.Gateway;
import co.votacion.nodo.ConfigRaft;
import co.votacion.nodo.NodoRaft;
import co.votacion.nodo.ServidorNodo;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Pruebas de integración: 3 nodos y el gateway en la misma JVM, cada uno con su
 * propio RMI Registry en un puerto distinto. La comunicación es RMI real por TCP.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ClusterTest {

    private static final int BASE = 15100;
    private static final String GW = "http://localhost:" + BASE;
    private static final ConfigRaft RAPIDO = new ConfigRaft(50, 250, 500, 2000);

    private static final List<DireccionNodo> DIRS = List.of(
            new DireccionNodo(1, "localhost", BASE + 1),
            new DireccionNodo(2, "localhost", BASE + 2),
            new DireccionNodo(3, "localhost", BASE + 3));

    private static final List<ServidorNodo> nodos = new ArrayList<>();
    private static Gateway gateway;
    private static final HttpClient http = HttpClient.newHttpClient();
    private static final ObjectMapper json = new ObjectMapper();

    @BeforeAll
    static void levantar() throws Exception {
        ClienteRmi.configurarTimeouts(300, 5000);
        for (DireccionNodo d : DIRS) {
            List<DireccionNodo> pares = DIRS.stream().filter(p -> p.id() != d.id()).toList();
            nodos.add(new ServidorNodo(d.id(), d.puerto(), pares, RAPIDO, true));
        }
        gateway = new Gateway(BASE, DIRS, true);
        gateway.iniciar();
    }

    @AfterAll
    static void bajar() {
        gateway.detener();
        nodos.forEach(ServidorNodo::detener);
    }

    // ------------------------------------------------------------------ utilidades

    record Http(int status, Map<String, Object> cuerpo) {
    }

    @SuppressWarnings("unchecked")
    static Http api(String metodo, String ruta, Object cuerpo) throws Exception {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(GW + ruta)).header("Content-Type", "application/json");
        b = cuerpo == null ? b.method(metodo, HttpRequest.BodyPublishers.noBody())
                : b.method(metodo, HttpRequest.BodyPublishers.ofString(json.writeValueAsString(cuerpo)));
        HttpResponse<String> r = http.send(b.build(), HttpResponse.BodyHandlers.ofString());
        return new Http(r.statusCode(), json.readValue(r.body(), Map.class));
    }

    static Map<String, String> voto(String nombre, String cedula, String candidato) {
        return Map.of("nombre", nombre, "cedula", cedula, "candidato", candidato);
    }

    static NodoRaft raft(int id) {
        return nodos.get(id - 1).raft();
    }

    static NodoRaft esperarLider(Integer excluir) throws InterruptedException {
        for (int i = 0; i < 200; i++) {
            List<NodoRaft> lideres = nodos.stream().map(ServidorNodo::raft)
                    .filter(r -> r.rol() == NodoRaft.Rol.LIDER && !r.estaPausado() && !Integer.valueOf(r.id()).equals(excluir))
                    .toList();
            if (lideres.size() == 1) return lideres.get(0);
            Thread.sleep(25);
        }
        throw new AssertionError("No se eligió líder");
    }

    static void esperarHasta(BooleanSupplier condicion, String descripcion) throws InterruptedException {
        long fin = System.currentTimeMillis() + 5000;
        while (System.currentTimeMillis() < fin) {
            if (condicion.getAsBoolean()) return;
            Thread.sleep(25);
        }
        fail("Condición no alcanzada: " + descripcion + " -> " + nodos.stream().map(n -> n.raft().estado()).toList());
    }

    static int votosEn(int id) {
        return raft(id).leer(m -> m.total());
    }

    // ------------------------------------------------------------------ pruebas

    @Test
    @Order(1)
    void seEligeExactamenteUnLider() throws Exception {
        NodoRaft lider = esperarLider(null);
        esperarHasta(() -> nodos.stream().allMatch(n -> Integer.valueOf(lider.id()).equals(n.raft().liderId())),
                "todos reconocen al líder");
    }

    @Test
    @Order(2)
    void registraUnVotoYLoReplicaEnTodos() throws Exception {
        Http r = api("POST", "/api/votos", voto("Ana Pérez", "1012345678", "C1"));
        assertEquals(200, r.status(), r.cuerpo().toString());
        esperarHasta(() -> votosEn(1) == 1 && votosEn(2) == 1 && votosEn(3) == 1, "3 réplicas");

        Http res = api("GET", "/api/resultados", null);
        assertEquals(1, res.cuerpo().get("total"));
    }

    @Test
    @Order(3)
    void unaCedulaNoPuedeVotarDosVecesAunqueEntrePorUnSeguidor() throws Exception {
        NodoRaft seguidor = nodos.stream().map(ServidorNodo::raft).filter(r -> r.rol() != NodoRaft.Rol.LIDER)
                .findFirst().orElseThrow();
        ClienteRmi cliente = new ClienteRmi(DIRS.get(seguidor.id() - 1));
        VotoRechazadoException e = assertThrows(VotoRechazadoException.class, () -> cliente.llamar(
                n -> n.registrarVoto(new SolicitudRegistro("Ana Pérez", "1012345678", "C2", null))));
        assertEquals(VotoRechazadoException.Codigo.CEDULA_DUPLICADA, e.getCodigo());

        assertEquals(409, api("POST", "/api/votos", voto("Ana Pérez", "1012345678", "C2")).status());
    }

    @Test
    @Order(4)
    void rechazaDatosInvalidos() throws Exception {
        Http r = api("POST", "/api/votos", voto("X", "abc", "C9"));
        assertEquals(400, r.status());
        assertEquals(3, ((List<?>) r.cuerpo().get("detalles")).size());
    }

    @Test
    @Order(5)
    void siElLiderCaeSeEligeOtroYSeSigueVotando() throws Exception {
        NodoRaft viejo = esperarLider(null);
        long terminoViejo = viejo.termino();
        assertEquals(200, api("POST", "/api/cluster/" + viejo.id() + "/pausar", null).status());

        NodoRaft nuevo = esperarLider(viejo.id());
        assertNotEquals(viejo.id(), nuevo.id());
        assertTrue(nuevo.termino() > terminoViejo);

        Http r = api("POST", "/api/votos", voto("Bruno Díaz", "79111222", "C2"));
        assertEquals(200, r.status(), r.cuerpo().toString());
        assertEquals(nuevo.id(), r.cuerpo().get("lider"));
        assertEquals(1, votosEn(viejo.id()), "el nodo caído no recibe el voto nuevo");

        // Al volver, el nodo se pone al día y reconoce al nuevo líder.
        api("POST", "/api/cluster/" + viejo.id() + "/reanudar", null);
        esperarHasta(() -> votosEn(viejo.id()) == 2 && viejo.rol() == NodoRaft.Rol.SEGUIDOR, "nodo recuperado");
        esperarHasta(() -> new HashSet<>(nodos.stream().map(n -> n.raft().idsConfirmados()).toList()).size() == 1,
                "logs confirmados idénticos");
    }

    @Test
    @Order(6)
    void sinMayoriaNoSeAceptanVotos() throws Exception {
        NodoRaft lider = esperarLider(null);
        List<NodoRaft> seguidores = nodos.stream().map(ServidorNodo::raft).filter(r -> r != lider).toList();
        seguidores.forEach(NodoRaft::pausar);
        int antes = votosEn(lider.id());

        ClienteRmi cliente = new ClienteRmi(DIRS.get(lider.id() - 1));
        VotoRechazadoException e = assertThrows(VotoRechazadoException.class, () -> cliente.llamar(
                n -> n.registrarVoto(new SolicitudRegistro("Carla Ríos", "55566677", "C1", null))));
        assertEquals(VotoRechazadoException.Codigo.TIMEOUT, e.getCodigo());
        assertEquals(antes, votosEn(lider.id()), "sin mayoría no se confirma nada");

        seguidores.forEach(NodoRaft::reanudar);
        esperarLider(null);
    }

    @Test
    @Order(7)
    void votosConcurrentesPorDistintosNodosQuedanEnElMismoOrden() throws Exception {
        esperarLider(null);
        ExecutorService pool = Executors.newFixedThreadPool(10);
        List<Callable<Object>> tareas = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            final int k = i;
            tareas.add(() -> new ClienteRmi(DIRS.get(k % 3)).llamar(n -> n.registrarVoto(
                    new SolicitudRegistro("Votante Numero", String.valueOf(30000000 + k), k % 2 == 0 ? "C1" : "C2", null))));
        }
        for (Future<Object> f : pool.invokeAll(tareas)) assertNotNull(f.get());
        pool.shutdown();

        int total = esperarLider(null).leer(m -> m.total());
        esperarHasta(() -> votosEn(1) == total && votosEn(2) == total && votosEn(3) == total, "réplicas completas");
        var ordenes = new HashSet<>(nodos.stream().map(n -> n.raft().leer(m -> m.cedulasEnOrden())).toList());
        assertEquals(1, ordenes.size(), "todos los nodos aplican los votos en el mismo orden");
    }
}

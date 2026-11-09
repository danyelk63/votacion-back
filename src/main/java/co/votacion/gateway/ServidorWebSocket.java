package co.votacion.gateway;

import co.votacion.comun.Evento;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class ServidorWebSocket extends WebSocketServer {
    private final CentralEventos central;
    private final ObjectMapper json;
    private final Consumer<Evento> difusor = this::difundir;
    private final Consumer<String> log;

    public ServidorWebSocket(int puerto, CentralEventos central, ObjectMapper json, Consumer<String> log) {
        super(new InetSocketAddress(puerto));
        this.central = central;
        this.json = json;
        this.log = log;
        setReuseAddr(true);
        setConnectionLostTimeout(30);
    }

    @Override
    public void onStart() {
        central.suscribir(difusor);

        central.alVaciar(() -> broadcast("{\"tipo\":\"reinicio\"}"));
        log.accept("WebSocket de eventos escuchando en ws://localhost:" + getPort());
    }

    @Override
    public void onOpen(WebSocket conexion, ClientHandshake handshake) {
        enviar(conexion, Map.of("tipo", "historial", "eventos", central.historial()));
    }

    private void difundir(Evento e) {
        try {
            broadcast(json.writeValueAsString(Map.of("tipo", "evento", "evento", e)));
        } catch (Exception ignorada) {
        }
    }

    private void enviar(WebSocket c, Object mensaje) {
        try {
            c.send(json.writeValueAsString(mensaje));
        } catch (Exception ignorada) {
        }
    }

    public int clientes() {
        return getConnections().size();
    }

    @Override
    public void onClose(WebSocket conexion, int codigo, String razon, boolean remoto) {
    }

    @Override
    public void onMessage(WebSocket conexion, String mensaje) {
        if ("historial".equals(mensaje)) enviar(conexion, Map.of("tipo", "historial", "eventos", List.copyOf(central.historial())));
    }

    @Override
    public void onError(WebSocket conexion, Exception ex) {
        log.accept("error WebSocket: " + ex.getMessage());
    }

    public void detener() {
        central.desuscribir(difusor);
        try {
            stop(500);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}

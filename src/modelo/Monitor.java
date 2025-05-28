package modelo;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import conexion.ListaSDTO;
import conexion.Paquete;
import conexion.PuertoDTO;
import utils.ConfigLoader;

public class Monitor {
    public static final int PROXY_PORT = ConfigLoader.port;
    private final ServerSocket listenSocket;
    private final List<ServidorActivo> backends = new CopyOnWriteArrayList<>();
    private final ScheduledExecutorService heartbeatExec = Executors.newSingleThreadScheduledExecutor();
    
    
    public Monitor() throws IOException {
        this.listenSocket = new ServerSocket(PROXY_PORT);
        // Arrancamos heartbeat: cada 5s ping a todos los backends registrados
        heartbeatExec.scheduleAtFixedRate(this::runHeartbeat, 5, 5, TimeUnit.SECONDS);
    }

    /** Bucle principal: acepta conexiones entrantes (clientes y servidores). */
    private void acceptLoop() {
        while (true) {
            try {
                Socket sock = listenSocket.accept();
                new Thread(() -> handleConnection(sock)).start();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /** Lee la primera línea para decidir la operación y despacha. */
    private void handleConnection(Socket socket) {
        try {
        	ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
    		out.flush();
        	ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
			System.out.println("[Monitor] Conectado desde "+socket.getInetAddress() + ":" + socket.getPort());

			Paquete paq;

			while ((paq = (Paquete) in.readObject()) != null) {
        		String op = paq.getOperacion();
        		System.out.println(op);

        		switch (op) {
        		case "obtenerSA": //llega desde el server
        			enviarServidorActivo(out); //recibe ack en reintento
        			break;
        		case "registrarS": //llega desde el server
        			registrarServidor((PuertoDTO)paq.getContenido());
        	    	enviarListaDeServidores(out);  //recibe ack en reintento
        			break;
        		case "obtenerS":
        			enviarListaDeServidores(out);  //recibe ack en reintento
        			break;
        		default:
        			System.out.println("ERROR: operacion desconocida");
        		}
			}
    		socket.close();
        } catch (IOException e) {
        	//error en la conexion de los buffers
        	System.out.println("Se cerro la conexion desde el monitor");
//			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			//error en la lectura del objeto
			e.printStackTrace();
		}
    }    
    
    private void enviarListaDeServidores(ObjectOutputStream out) {
		Iterator<ServidorActivo> it = backends.iterator();
		Paquete paq = new Paquete("obtenerSR", new ListaSDTO());
		ArrayList<PuertoDTO> listaPuertos = new ArrayList<PuertoDTO>();
				
    	while(it.hasNext()) {
    		ServidorActivo sA = it.next();
    		PuertoDTO pDTO = new PuertoDTO(sA.getPuerto(), sA.getIp());
	    	listaPuertos.add(pDTO);
    	}
    	((ListaSDTO) paq.getContenido()).setServidores(listaPuertos);

    	try {
			out.writeObject(paq);
        	out.flush();
		} catch (IOException e) {
			System.err.println("Falló la conexión al servidor");
			if(reintento(paq) == null) {
				System.err.println("No hay servidores disponibles");
			}
		}
	}

	private void registrarServidor(PuertoDTO contenido) {
    	ServidorActivo sA = new ServidorActivo(contenido.getAddress(), contenido.getPuerto());
    	backends.add(sA);
	}

	private void enviarServidorActivo(ObjectOutputStream out) {
		Paquete paq;
		if (backends.isEmpty()) {
		   System.err.println("No hay servidores activos registrados");
		   paq = new Paquete("obtenerSAR", null);
		}else {
			ServidorActivo s = backends.get(0);
	    	paq = new Paquete("obtenerSAR", new PuertoDTO(s.getPuerto(), s.getIp()));
			}
		try {
			out.writeObject(paq);
        	out.flush();
		} catch (IOException e) {
			System.err.println("Falló la conexión al servidor");
			if(reintento(paq) == null) {
				System.err.println("No hay servidores disponibles");
			}
		}
	}	

	/** Cada 5s hacemos ping a todos los backends para mantener vivos/muertos */
    private void runHeartbeat() {
        for (ServidorActivo sc : backends) {
            if (!sc.checkHeartbeat()) {
                System.err.println("[PROXY] Backend no responde: " + sc);
                sc.markDead();
            } else {
                sc.markAlive();
            }
        }
    }

    private String reintento(Paquete paq) {
        if (backends.isEmpty()) { return null; }
        int cantServidores = backends.size();
        for (int i = 0; i < cantServidores; i++) {
            ServidorActivo sc = backends.get(0);

            if (!sc.isAlive()) {
                System.err.println("[PROXY] Backend muerto, removiendo: " + sc);
                backends.remove(0);
                continue;
            }
            try {
                // 5) Intentamos enviar (3 reintentos internos)
                String resp = sc.sendAndAwaitAck(paq);
                return resp;  
            } catch (IOException e) {
                System.err.println("[PROXY] Falló primer backend " + sc);
                backends.remove(0);
            }
        }
        return null;
    }

    
    
    public static void main(String[] args) throws IOException {
        System.out.println("[PROXY] Arrancando proxy en puerto " + PROXY_PORT);
        new Monitor().acceptLoop();
    }
}

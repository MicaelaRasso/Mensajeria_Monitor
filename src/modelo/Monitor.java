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

			Paquete paq;

			while ((paq = (Paquete) in.readObject()) != null) {
        		String op = paq.getOperacion();
        		if(!backends.isEmpty()) {
            		ServidorActivo sA = backends.get(0);
        			System.out.println("Servidor activo: "+sA.getIp()+":"+sA.getPuerto());
        		}
        		switch (op) {
        		case "obtenerSA": //llega desde el server
        			enviarServidorActivo(out); //recibe ack en reintento
        			break;
        		case "registrarS": //llega desde el server
        			registrarServidor((PuertoDTO)paq.getContenido());
        	    	enviarListaDeServidores(out);  //recibe ack en reintento
        	    	dormir(1000);
        	    	actualizarServidores();
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
//        	System.out.println("Se cerro la conexion desde el monitor");
//			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			//error en la lectura del objeto
			e.printStackTrace();
		}
    }    
    
    private void dormir(int i) {
    	try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {}
	}

	private void actualizarServidores() {
    	ListaSDTO lSDTO = new ListaSDTO();
    	PuertoDTO pDTO;
    	ArrayList<PuertoDTO> nuevaLista = new ArrayList<PuertoDTO>();
    	for(ServidorActivo s: backends) {
    		pDTO = new PuertoDTO(s.getPuerto(),s.getIp());
    		nuevaLista.add(pDTO);
    	}
    	lSDTO.setServidores(nuevaLista);
    	Paquete paquete = new Paquete("actualizarS",lSDTO);
    	
    	for(ServidorActivo s: backends) {
    		s.actualizarServidores(paquete);
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
	    String nuevaIP = contenido.getAddress();
	    int nuevoPuerto = contenido.getPuerto();

	    // Verificar si ya existe un servidor con esa IP y puerto
	    for (ServidorActivo s : backends) {
	        if (s.getPuerto() == nuevoPuerto && s.getIp().equals(nuevaIP)) {
	            System.out.println("[Monitor] Reconexión del servidor: " + nuevaIP + ":" + nuevoPuerto);
	            s.markAlive();
	            return;
	        }
	    }

	    // Si no estaba, se registra como nuevo
	    ServidorActivo nuevo = new ServidorActivo(nuevaIP, nuevoPuerto);
	    backends.add(nuevo);
	    System.out.println("[Monitor] Nuevo servidor registrado: " + nuevaIP + ":" + nuevoPuerto);
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
	    for (ServidorActivo sc : new ArrayList<>(backends)) {
	        if (!sc.checkHeartbeat()) {
	            System.err.println("[Monitor] Backend no responde: " + sc);
	            backends.remove(sc);
	            backends.add(sc);      
	            sc.markDead();
	        } else {
	            sc.markAlive();
	        }
	    }
	}
	private String reintento(Paquete paq) {
	    if (backends.isEmpty()) return null;

	    List<ServidorActivo> copia = new ArrayList<>(backends);

	    for (ServidorActivo sc : copia) {
	        if (!sc.isAlive()) {
	            System.err.println("[Monitor] Backend muerto, moviendo: " + sc);
	            backends.remove(sc);
	            backends.add(sc);
	            continue;
	        }
	        try {
	            String resp = sc.sendAndAwaitAck(paq);
	            return resp; 
	        } catch (IOException e) {
	            System.err.println("[Monitor] Falló backend al enviar paquete: " + sc);
	            backends.remove(sc);
	            backends.add(sc);
	        }
	    }
	    return null;
	}

    
    
    public static void main(String[] args) throws IOException {
        System.out.println("[PROXY] Arrancando proxy en puerto " + PROXY_PORT);
        new Monitor().acceptLoop();
    }
}

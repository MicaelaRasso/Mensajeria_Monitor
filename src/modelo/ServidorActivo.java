package modelo;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.TimeUnit;

import conexion.Paquete;

/**
 * Representa una réplica de servidor backend:
 *  - Mantiene IP/puerto
 *  - checkHeartbeat(): un simple TCP connect+close
 *  - sendAndAwaitAck(): envía la petición, espera "ACK" como primera línea
 *      y luego retorna la respuesta que venga a continuación.
 */

public class ServidorActivo {
	    final String ip;
	    final int puerto;
	    private volatile boolean alive = true;

	    public ServidorActivo(String ip, int port) {
	        this.ip   = ip;
	        this.puerto = port;
	    }

	    /** Intento de ping: abrimos+cerramos socket en timeout corto */
	    public boolean checkHeartbeat() {
	        try (Socket s = new Socket()) {
	            s.connect(new InetSocketAddress(ip, puerto), 1000);
	            return true;
	        } catch (IOException e) {
	            return false;
	        }
	    }

	    /** Marca esta réplica como caída */
	    public void markDead()  { alive = false; }
	    /** Marca como viva (tras heartbeat OK) */
	    public void markAlive() { alive = true; }
	    public boolean isAlive(){ return alive; }

	    @Override
	    public String toString() {
	        return ip + ":" + puerto;
	    }

	    /**
	     * Envía `req` y espera:
	     *   1) primera línea "ACK"
	     *   2) luego lee la segunda línea como la respuesta efectiva
	     * Internamente reintenta HASTA 3 veces si no recibe ACK en ≤1s.
	     */
	    public String sendAndAwaitAck(Paquete paq) throws IOException {
	        Exception lastEx = null;

	        for (int attempt = 1; attempt <= 3; attempt++) {
	            try (
	                Socket s  = new Socket();
	            ) {
	                s.connect(new InetSocketAddress(ip, puerto), 1000);
	                s.setSoTimeout(1000);
	    			ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream());
	    			out.flush();
	                ObjectInputStream in = new ObjectInputStream(s.getInputStream());
	        		
	                // 1) Enviar petición
	                out.writeObject(paq);

	                // 2) Esperar ACK
	                Paquete paq1 = (Paquete) in.readObject();
	                System.out.println(paq1.getOperacion());
	                if (!"ACK".equalsIgnoreCase(paq1.getOperacion())) {
	                    throw new IOException("No vino ACK (vino: " + paq1.getOperacion() + ")");
	                }
/*	                // 3) Leer respuesta real
	                String resp = in.readLine();
	                System.out.println("respuesta " + resp);
	                return resp != null ? resp : "";
*/
	            } catch (IOException | ClassNotFoundException ioe) {
	                lastEx = ioe;
	                // pequeña espera antes del retry
	                try { TimeUnit.MILLISECONDS.sleep(200); } catch (InterruptedException ignored) {}
	            }
	        }

	        // si agotamos retries, lanzamos
	        throw new IOException("No ACK tras 3 intentos", lastEx);
	    }

		public String getIp() {
			return ip;
		}

		public int getPuerto() {
			return puerto;
		}

		public void actualizarServidores(Paquete paquete) {
			try (Socket s = new Socket()) {
				s.connect(new InetSocketAddress(ip, puerto), 1000);
				s.setSoTimeout(1000);
				ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream());
				out.flush();
				ObjectInputStream in = new ObjectInputStream(s.getInputStream());
				
				out.writeObject(paquete);
			} catch (IOException e) {
				System.err.println("No se pudo conectar al servidor: " + ip + "@" + puerto);
				e.printStackTrace();
			}
		}
}
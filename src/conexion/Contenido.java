package conexion;

import java.io.Serializable;
import java.time.LocalDateTime;

public abstract class Contenido implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private LocalDateTime fechaYHora;
	
	public Contenido () {
		this.fechaYHora = LocalDateTime.now();
	}

	public LocalDateTime getFechaYHora() {
		return fechaYHora;
	}

	public void setFechaYHora(LocalDateTime fechaYHora) {
		this.fechaYHora = fechaYHora;
	}

	@Override
	public String toString() {
		return "fyh:" + fechaYHora;
	}
	
	

}

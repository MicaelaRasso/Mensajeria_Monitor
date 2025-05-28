package conexion;

import java.io.Serializable;

public class Paquete implements Serializable {
    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String operacion;
    private Contenido contenido;

    public Paquete() {}
    
    public Paquete(String operacion, Contenido contenido) {
		this.operacion = operacion;
		this.contenido = contenido;
	}

	// Getters y Setters
    public String getOperacion() {
        return operacion;
    }

    public void setOperacion(String operacion) {
        this.operacion = operacion;
    }

    public Contenido getContenido() {
        return contenido;
    }

    public void setContenido(Contenido contenido) {
        this.contenido = contenido;
    }

	@Override
	public String toString() {
		return "[Paquete] op: " + operacion + " contenido: " + contenido;
	}
    
    
}

package conexion;

public class UsuarioDTO extends Contenido {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String nombre;
	private String respuesta;
	
	public UsuarioDTO(String nombre) {
		super();
		this.nombre = nombre;
		respuesta = "";
	}

	public String getRespuesta() {
		return respuesta;
	}

	public void setRespuesta(String respuesta) {
		this.respuesta = respuesta;
	}
	
	public String getNombre() {
		return nombre;
	}
	
	@Override
	public String toString() {
			return " nombre: " + nombre + " respuesta: " + respuesta;
	}
	
	
}

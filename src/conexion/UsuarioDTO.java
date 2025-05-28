package conexion;

public class UsuarioDTO extends Contenido {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String nombre;
	private PuertoDTO puerto;
	private String respuesta;

	public UsuarioDTO(String nombreC, PuertoDTO puerto) {
		super();
		this.nombre = nombreC;
		this.puerto = puerto;
		respuesta = "";
	}
	
	public UsuarioDTO(String nombre) {
		super();
		this.nombre = nombre;
		this.puerto = null;
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
	
	public PuertoDTO getPuerto() {
		return puerto;
	}
}

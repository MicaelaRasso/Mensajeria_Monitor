package conexion;

public class ContenidoC extends Contenido {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private UsuarioDTO emisor;

	public ContenidoC(UsuarioDTO emisor) {
		super();
		this.emisor = emisor;
	}

	public UsuarioDTO getEmisor() {
		return emisor;
	}

	public void setEmisor(UsuarioDTO emisor) {
		this.emisor = emisor;
	}

	@Override
	public String toString() {
		return " emisor: " + emisor;
	}

}

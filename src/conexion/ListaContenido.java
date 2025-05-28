package conexion;

import java.util.ArrayList;

public class ListaContenido extends Contenido {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private ArrayList<Contenido> contenido;

	public ListaContenido() {
		super();
		this.contenido = new ArrayList<Contenido>();
	}

	public ArrayList<Contenido> getContenidoC() {
		return contenido;
	}
	
	public void setContenido(ArrayList<Contenido> contenido) {
		this.contenido = contenido;
	}

	@Override
	public String toString() {
		return " contenido: " + contenido.toString();
	}

}

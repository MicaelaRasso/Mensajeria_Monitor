package conexion;

import java.util.ArrayList;

public class ListaSDTO extends Contenido {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private ArrayList<PuertoDTO> servidores;

	public ListaSDTO() {
		this.servidores = new ArrayList<PuertoDTO>();
	}

	public ArrayList<PuertoDTO> getServidores() {
		return servidores;
	}

	public void setServidores(ArrayList<PuertoDTO> servidores) {
		this.servidores = servidores;
	}

}

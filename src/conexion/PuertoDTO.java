package conexion;

public class PuertoDTO extends Contenido {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private int puerto;
	private String address;
	
	public PuertoDTO(int puerto, String address) {
		super();
		this.setPuerto(puerto);
		this.setAddress(address);
	}

	public int getPuerto() {
		return puerto;
	}

	public void setPuerto(int puerto) {
		this.puerto = puerto;
	}

	public String getAddress() {
		return address;
	}

	public void setAddress(String address) {
		this.address = address;
	}

	@Override
	public String toString() {
		return " puerto: " + puerto + " address: " + address;
	}
	
	
}

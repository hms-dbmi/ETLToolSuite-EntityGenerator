package etl.exceptions;

public class RequiredFieldException extends Exception {

	public RequiredFieldException(String string) {
		super(string);
	}

	public RequiredFieldException() {
		super();
		
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 8869018401260226923L;

}

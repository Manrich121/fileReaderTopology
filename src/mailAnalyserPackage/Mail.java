package mailAnalyserPackage;

import java.io.Serializable;

/**
 * Implementation of a reddit post
 * @author Luke Barnett 1109967
 *
 */
public class Mail implements Serializable {
	
	/**
	 * Generated serialVersionUID
	 */
	private static final long serialVersionUID = -1176274406450812313L;
	private String label;
	private String data;
	
	/**
	 * Creates a new Post object
	 * @param title The title of the post
	 * @param subReddit The sub-reddit the post came from
	 */
	public Mail(String title, String subReddit){
		this.data = title;
		this.label = subReddit;
	}
	
	/**
	 * Creates an empty Post object
	 */
	public Mail(){}
	
	/**
	 * Gets the sub-reddit the post came from
	 * @return The sub-reddit of the post
	 */
	public String getLabel(){
		return this.label;
	}

	/**
	 * Gets the title of the post
	 * @return The title of the post
	 */
	public String getData(){
		return this.data;
	}
	
	/**
	 * Sets the sub-reddit the post came from
	 * @param subReddit The sub-reddit to set the post from
	 */
	public void setLabel(String label){
		this.label = label;
	}
	
	/**
	 * Sets the title of the post
	 * @param data The title of the post to set
	 */
	public void setData(String data){
		this.data = data;
	}
	
	@Override
	public String toString(){
		return String.format("%s [%s]", this.data, this.label);
	}

}

package etl.data.datasource.entities;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.jdom2.Element;

public class CriteriaNode {
	
	private Object parent;
	
	private List<Object> inclusionNodes;

	public Object getParent() {
		return parent;
	}

	public void setParent(Object parent) {
		this.parent = parent;
	}

	public List<Object> getInclusionNodes() {
		return inclusionNodes;
	}

	public void setInclusionNodes(List<Object> inclusionNodes) {
		this.inclusionNodes = inclusionNodes;
	}

	@Override
	public String toString() {
		return "CriteriaNode [parent=" + parent + ", inclusionNodes="
				+ inclusionNodes + "]";
	}
	
	public Set<String> getParents(Collection<CriteriaNode> crits){
		
		Set<String> set = new HashSet<String>();
		
		crits.forEach(crit ->{
			set.add(crit.getParent().toString());
		});
				 
		return set;
	}
	
}

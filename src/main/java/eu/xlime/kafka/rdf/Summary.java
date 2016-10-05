package eu.xlime.kafka.rdf;

import java.util.Map;
import java.util.HashMap;
import java.util.Date;
import java.util.Calendar;

public class Summary {
	private String consumerId;
	private Map<String,Long> counters;
	private Date date; 
	
	public Summary(){
		this.date = Calendar.getInstance().getTime();
		this.counters = new HashMap<String,Long>();
	}

	public String getConsumerId() {
		return this.consumerId;
	}

	public void setConsumerId(String consumerId) {
		this.consumerId = consumerId;
	}

	public Map<String,Long> getCounters(){
		return this.counters;
	}
	
	public void setCounter(String name, Long count){
		this.counters.put(name, count);
	}
	
	public void addCounters(Map<String,Long> counters){
		for (Map.Entry<String,Long> entry: counters.entrySet()){
			this.counters.put(entry.getKey(), entry.getValue());
		}
	}

	public Date getDate(){
		return this.date;
	}

	public String toString() {
		if (this.counters != null){
			return "Summary: Consumer " + this.consumerId + ", date: " + this.date + "\n\t"
				+ this.counters.toString();
		}else{
			return "No xLiMe resources read or stored yet";
		}
	}
}

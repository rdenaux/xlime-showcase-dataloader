package eu.xlime.kafka.rdf;

import java.util.Map;
import java.util.Date;
import java.util.Calendar;

public class Summary {
	private String consumerId;
	private Map<String,Long> annotations;
	private long processed;
	private Date date; 
	
	public Summary(){
		date = Calendar.getInstance().getTime();
	}

	public String getConsumerId() {
		return consumerId;
	}

	public void setConsumerId(String consumerId) {
		this.consumerId = consumerId;
	}

	public Map<String,Long> getAnnotations(){
		return annotations;
	}
	
	public void setAnnotations(Map<String,Long> annotations){
		this.annotations = annotations;
	}
	
	public long getProcessed() {
		return processed;
	}

	public void setProcessed(long processed) {
		this.processed = processed;
	}
	
	public Date getDate(){
		return this.date;
	}

	public String toString() {
		if (this.annotations != null){
			return "Summary: Processed " + processed + " messages for Consumer " + consumerId + " on " + date + "\n\t"
				+ annotations.toString();
		}else{
			return "No xLiMe resources read or stored yet";
		}
	}
}

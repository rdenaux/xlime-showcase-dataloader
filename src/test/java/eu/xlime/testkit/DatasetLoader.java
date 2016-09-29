package eu.xlime.testkit;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;

import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.io.Files;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.DatasetFactory;

public class DatasetLoader {

	private static Logger log = LoggerFactory.getLogger(DatasetLoader.class);
	
	public Optional<Dataset> loadDataset(File file, Lang lang) {
		try {
			String rdfContent = Files.toString(file, Charsets.UTF_8);
//			System.out.println("rdfcontent (" + rdfContent.length() + "bytes):\n"+rdfContent+ "\n------end rdfContent-----");
			return loadDataset(new StringReader(rdfContent),lang);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	public Optional<Dataset> loadDataset(StringReader reader, Lang lang) {
		Dataset dataset = DatasetFactory.createMem();
		try {
			String baseUri = "";
			RDFDataMgr.read(dataset, reader, baseUri, lang);
		} catch (Exception e) {
			log.error("Failed to parse RDF from input stream", e);
			dataset.close();
			return Optional.absent();
		}
		return Optional.of(dataset);
	}
	
}

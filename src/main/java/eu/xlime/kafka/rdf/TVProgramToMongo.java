package eu.xlime.kafka.rdf;

import java.util.List;
import java.util.Properties;

import kafka.message.MessageAndMetadata;

import com.hp.hpl.jena.query.Dataset;

import eu.xlime.bean.TVProgramBean;
import eu.xlime.bean.XLiMeResource;
import eu.xlime.kafka.ConfigOptions;
import eu.xlime.kafka.RunExtractor;
import eu.xlime.kafka.msgproc.DatasetProcessor;

/**
 * {@link DatasetProcessor} meant to be used in an xLiMe Kafka {@link RunExtractor} execution.
 *  
 * It expects a {@link Dataset} from an xLiMe (EPG) stream and it will extract {@link TVProgramBean}s;
 * then it will push those beans to a MongoDB. See {@link ConfigOptions} for configuration keys.
 *  
 * @author rdenaux
 *
 */
public class TVProgramToMongo extends BaseXLiMeResourceToMongo {

	public TVProgramToMongo(Properties props) {
		super(props);
	}

	@Override
	protected List<? extends XLiMeResource> extractXLiMeResourceBeans(
			MessageAndMetadata<byte[], byte[]> mm, Dataset dataset) {
		return extractXLiMeMediaItems(mm, dataset, TVProgramBean.class);
	}

}

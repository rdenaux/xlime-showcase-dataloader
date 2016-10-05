package eu.xlime.kafka.rdf;

import java.util.List;
import java.util.Properties;

import kafka.message.MessageAndMetadata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.hp.hpl.jena.query.Dataset;

import eu.xlime.bean.ASRAnnotation;
import eu.xlime.bean.XLiMeResource;
import eu.xlime.dao.annotation.MediaItemAnnotationDaoFromDataset;
import eu.xlime.kafka.ConfigOptions;
import eu.xlime.kafka.RunExtractor;
import eu.xlime.kafka.msgproc.DatasetProcessor;
import eu.xlime.util.ResourceTypeResolver;

/**
 * {@link DatasetProcessor} that processes the ASR stream in an xLiMe Kafka {@link RunExtractor} execution.
 *  
 * It expects a {@link Dataset} from an xLiMe (ASR) stream and it will extract {@link ASRAnnotation}s;
 * then it will push those beans to a MongoDB. See {@link ConfigOptions} for configuration keys.
 * 
 * @author rdenaux
 *
 */
public class ASRToMongo extends BaseXLiMeResourceToMongo {

	private static final Logger log = LoggerFactory.getLogger(ASRToMongo.class);
	
	private static final ResourceTypeResolver typeResolver = new ResourceTypeResolver();
	
	public ASRToMongo(Properties props){
		super(props);
	}
	
	@Override
	protected List<? extends XLiMeResource> extractXLiMeResourceBeans(
			MessageAndMetadata<byte[], byte[]> mm, Dataset dataset) {
		return extractASRAnnotations(dataset);
	}

	private List<? extends XLiMeResource> extractASRAnnotations(
			Dataset dataset) {
		ImmutableList.Builder<XLiMeResource> builder = ImmutableList.builder();
		MediaItemAnnotationDaoFromDataset annDao = new MediaItemAnnotationDaoFromDataset(dataset, kbEntityMapper);
		List<ASRAnnotation> asrAnns = annDao.findAllASRAnnotations(200);
		builder.addAll(asrAnns);
		/* ASR stream differs from subtitle stream in that the output of ASR is not entity-linked, so the following
		 * is currently not needed. 
		for (ASRAnnotation asrAnn: asrAnns) {
			try {
				List<EntityAnnotation> ea = annDao.findVideoTrackEntityAnnotations(typeResolver.extractVideoTrackUri(asrAnn));
				builder.addAll(ea);
			} catch (Exception e) {
				log.error("Failed to extract entity annotations for " + ocrAnn);
			}
		}
		*/
		return builder.build();
	}
	
}

package eu.xlime.kafka.rdf;

import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.message.MessageAndMetadata;

import com.google.common.collect.ImmutableList;
import com.hp.hpl.jena.query.Dataset;

import eu.xlime.bean.EntityAnnotation;
import eu.xlime.bean.MediaItem;
import eu.xlime.bean.MicroPostBean;
import eu.xlime.bean.XLiMeResource;
import eu.xlime.dao.annotation.MediaItemAnnotationDaoFromDataset;
import eu.xlime.kafka.ConfigOptions;
import eu.xlime.kafka.RunExtractor;
import eu.xlime.kafka.msgproc.DatasetProcessor;

/**
 * {@link DatasetProcessor} meant to be used in an xLiMe Kafka {@link RunExtractor} execution.
 *  
 * It expects a {@link Dataset} from an xLiMe (socialmedia) stream and it will extract {@link MicroPostBean}s;
 * then it will push those beans to a MongoDB. See {@link ConfigOptions} for configuration keys.
 *  
 * @author rdenaux
 *
 */
public class SocialMediaToMongo extends BaseXLiMeResourceToMongo {

	private static final Logger log = LoggerFactory.getLogger(SocialMediaToMongo.class);

	public SocialMediaToMongo(Properties props) {
		super(props);
	}

	@Override
	protected List<? extends XLiMeResource> extractXLiMeResourceBeans(
			MessageAndMetadata<byte[], byte[]> mm, Dataset dataset) {
		ImmutableList.Builder<XLiMeResource> builder = ImmutableList.builder();
		try {
			List<MediaItem> microPosts = extractXLiMeMediaItems(mm, dataset, MicroPostBean.class); 
			builder.addAll(microPosts);
			for (MediaItem mit: microPosts) {
				List<EntityAnnotation> entAnns = extractMicropostEntityAnnotations(dataset, mit.getUrl());
				setMentionDates(entAnns, microPosts);
				builder.addAll(entAnns);
			}
			//TODO: add activities
		} catch (Exception e) {
			log.error("Error processing message " + mm.offset(), e);
		}
		return builder.build(); 
	}
	
	private List<EntityAnnotation> extractMicropostEntityAnnotations(Dataset dataset, String microPostUrl) {
		try {
			MediaItemAnnotationDaoFromDataset miaDao = new MediaItemAnnotationDaoFromDataset(dataset, kbEntityMapper);
			return miaDao.findMicroPostEntityAnnotations(microPostUrl);
		} catch (Exception e) {
			log.error("Failed to extract EntityAnnotations for " + microPostUrl, e);
			return ImmutableList.of();
		}
	}
	
}

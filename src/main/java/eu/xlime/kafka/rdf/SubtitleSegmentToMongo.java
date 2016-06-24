package eu.xlime.kafka.rdf;

import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.message.MessageAndMetadata;

import com.google.common.collect.ImmutableList;
import com.hp.hpl.jena.query.Dataset;

import eu.xlime.bean.EntityAnnotation;
import eu.xlime.bean.SubtitleSegment;
import eu.xlime.bean.XLiMeResource;
import eu.xlime.dao.annotation.MediaItemAnnotationDaoFromDataset;
import eu.xlime.util.ResourceTypeResolver;

public class SubtitleSegmentToMongo extends BaseXLiMeResourceToMongo {

	private static final Logger log = LoggerFactory.getLogger(SubtitleSegmentToMongo.class);
	
	private static final ResourceTypeResolver typeResolver = new ResourceTypeResolver();
	
	public SubtitleSegmentToMongo(Properties props) {
		super(props);
	}

	@Override
	protected List<? extends XLiMeResource> extractXLiMeResourceBeans(
			MessageAndMetadata<byte[], byte[]> mm, Dataset dataset) {
		return extractSubtitleSegmentsAndAnnotations(dataset);
	}

	private List<? extends XLiMeResource> extractSubtitleSegmentsAndAnnotations(
			Dataset dataset) {
		ImmutableList.Builder<XLiMeResource> builder = ImmutableList.builder();
		MediaItemAnnotationDaoFromDataset annDao = new MediaItemAnnotationDaoFromDataset(dataset, kbEntityMapper);
		List<SubtitleSegment> sts = annDao.findAllSubtitleSegments(200);
		builder.addAll(sts);
		for (SubtitleSegment segment: sts) {
			try {
				List<EntityAnnotation> ea = annDao.findSubtitleTrackEntityAnnotations(typeResolver.extractSubtitleTrackUri(segment));
				builder.addAll(ea);
			} catch (Exception e) {
				log.error("Failed to extract entity annotations for " + segment);
			}
		}
		return builder.build();
	}
	
}

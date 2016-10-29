package eu.xlime.kafka.rdf;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import kafka.message.MessageAndMetadata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import com.hp.hpl.jena.query.Dataset;

import eu.xlime.bean.ASRAnnotation;
import eu.xlime.bean.EntityAnnotation;
import eu.xlime.bean.UIDate;
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
		for (ASRAnnotation asrAnn: asrAnns) {
			try {
				List<EntityAnnotation> ea = annDao.findAudioTrackEntityAnnotations(typeResolver.extractAudioTrackUri(asrAnn));
				ea = fixEntityAnnotations(ea, asrAnns);
				builder.addAll(ea);
			} catch (Exception e) {
				log.error("Failed to extract entity annotations for " + asrAnn);
			}
		}
		return builder.build();
	}

	private List<EntityAnnotation> fixEntityAnnotations(
			List<EntityAnnotation> eas, List<ASRAnnotation> asrAnns) {
		if (asrAnns.size() == 1) {
		// set EntityAnnotation.resourceURL to ASRAnnotation.url
			ASRAnnotation theAsr = asrAnns.get(0);
			//assert(theAsr is part of the audio track);
			for (EntityAnnotation ea : eas) {
				ea.setResourceUrl(theAsr.getUrl());
			}
		}
		Optional<Date> pubDate = extractPublicationDate(asrAnns);
		if (pubDate.isPresent()) {
			for (EntityAnnotation ea: eas) {
				ea.setMentionDate(new UIDate(pubDate.get()));
			}
		}
		
		return eas;
	}

	private Optional<Date> extractPublicationDate(List<ASRAnnotation> asrAnns) {
		List<Date> dates = new ArrayList<>();
		for (ASRAnnotation asrAnn: asrAnns) {
			try {
				Optional<Date> optDate = findResourcePubDate(asrAnn);
				if (optDate.isPresent()) dates.add(optDate.get());
			} catch (Exception e) {
				log.debug("Failed to extract publication date for asrAnn " + asrAnn);
			}
		}
		if (dates.isEmpty()) return Optional.absent();
		return Optional.of(Ordering.natural().reverse().sortedCopy(dates).get(0));
	}
	
}

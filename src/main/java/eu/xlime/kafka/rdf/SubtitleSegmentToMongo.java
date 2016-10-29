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

import eu.xlime.bean.EntityAnnotation;
import eu.xlime.bean.SubtitleSegment;
import eu.xlime.bean.UIDate;
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
				ea = fixEntityAnnotations(ea, sts);
				builder.addAll(ea);
			} catch (Exception e) {
				log.error("Failed to extract entity annotations for " + segment);
			}
		}
		return builder.build();
	}
	
	private List<EntityAnnotation> fixEntityAnnotations(
			List<EntityAnnotation> eas, List<SubtitleSegment> subtitles) {
		if (subtitles.size() == 1) {
		// set EntityAnnotation.resourceURL to ASRAnnotation.url
			SubtitleSegment theSub = subtitles.get(0);
			//assert(theAsr is part of the audio track);
			for (EntityAnnotation ea : eas) {
				ea.setResourceUrl(theSub.getUrl());
			}
		}
		Optional<Date> pubDate = extractPublicationDate(subtitles);
		if (pubDate.isPresent()) {
			for (EntityAnnotation ea: eas) {
				ea.setMentionDate(new UIDate(pubDate.get()));
			}
		}
		
		return eas;
	}

	private Optional<Date> extractPublicationDate(List<SubtitleSegment> subtitles) {
		List<Date> dates = new ArrayList<>();
		for (SubtitleSegment asrAnn: subtitles) {
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

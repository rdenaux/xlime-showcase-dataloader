package eu.xlime.dbpedia;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import jersey.repackaged.com.google.common.collect.ImmutableList;

import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.mongojack.DBCursor;
import org.mongojack.JacksonDBCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import com.google.common.io.LineProcessor;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.graph.impl.LiteralLabel;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.DatasetFactory;
import com.hp.hpl.jena.rdf.model.Model;
import com.mongodb.BasicDBObject;

import eu.xlime.bean.EntityAnnotation;
import eu.xlime.dao.MongoXLiMeResourceStorer;
import eu.xlime.summa.bean.UIEntity;
import eu.xlime.util.KBEntityMapper;
import eu.xlime.util.KBEntityUri;

public class AnnotatedUIEntityToMongo {

	private static final Logger log = LoggerFactory.getLogger(AnnotatedUIEntityToMongo.class);
	
	private final MongoXLiMeResourceStorer mongoStorer;

	public AnnotatedUIEntityToMongo(Properties props) {
		mongoStorer = new MongoXLiMeResourceStorer(props);
	}
	
	public Set<String> findEntityUrisInEntityAnnotations() {
		JacksonDBCollection<EntityAnnotation, String> eaColl = mongoStorer.getDBCollection(EntityAnnotation.class);
		BasicDBObject query = new BasicDBObject();
		BasicDBObject projection = new BasicDBObject("@type", "true").append("entity._id", "true").append("entity.@type", "true");
		DBCursor<EntityAnnotation> cursor = eaColl.find(query, projection);
		log.info(String.format("Found %s EntityAnnotations in Mongo", cursor.count()));
		ImmutableSet.Builder<String> builder = ImmutableSet.builder();
		long start = System.currentTimeMillis();
		while (cursor.hasNext()) {
			if (cursor.numSeen() % 10000 == 0) {
				log.info(String.format("Seen %s results", cursor.numSeen()));
			}
			try {
				EntityAnnotation entAnn = cursor.next();
				builder.add(entAnn.getEntity().getUrl());
			} catch (Exception e) {
				log.error("Failed to retrieve entity annotation for result " + cursor.numSeen());
			}
		}
		Set<String> result = builder.build();
		log.info(String.format("Extracted %s entityUris in %s ms", result.size(), (System.currentTimeMillis() - start)));
		return result;
	}
	
	public void writeEntityUrisToFile(Set<String> entUrls, File f) throws IOException {
		if (!f.exists()) {
			Files.createParentDirs(f);
			f.createNewFile();
		}
		final long start = System.currentTimeMillis();
		long cnt = 0;
		for (String entUrl: entUrls){
			try {
				Files.append(entUrl + "\n", f, Charsets.UTF_8);
				cnt++;
			} catch (IOException e) {
				log.error("Failed to append line for " + entUrl);
			}
		}
		log.info(String.format("Wrote %s lines (from %s) to %s in %s ms.", cnt++, entUrls.size(), f.getAbsolutePath(), (System.currentTimeMillis() - start)));
	}
	
	public List<String> readEntityUrisFromFile(File f) throws IOException {
		if (!f.exists()) throw new IllegalArgumentException("File must exist " + f);
		return Files.readLines(f, Charsets.UTF_8);
	}
	
	public Set<String> toCanonicalEntUris(Set<String> in) {
		Set<String> result = new HashSet<>();
		for (String entUrl: in) {
			KBEntityUri entUri = KBEntityUri.of(entUrl);
			result.add(entUri.asIri());
		}
		return ImmutableSet.copyOf(result);
	}
	
	public Map<File, EntityUpsertFromTTlSummary> loadToMongoFromDBpediaTtlFiles(Set<String> entUrls, String lang, File... dbpediaTtl) {
		final long start = System.currentTimeMillis();
		final Locale loc = new Locale(lang);

		final long startCount = mongoStorer.count(UIEntity.class, Optional.of(loc));
		log.info(String.format("Starting loading/updating %s entities to mongo from %s in %s", entUrls.size(), ImmutableList.copyOf(dbpediaTtl), loc));
		Map<File, EntityUpsertFromTTlSummary> summs = new HashMap<>();
		for (File dbpttl: dbpediaTtl) {
			try {
				summs.put(dbpttl, updateUIEntitiesInMongo(entUrls, loc, dbpttl));
			} catch (Exception e) {
				log.error("Failed to process " + dbpttl, e);
			}
		}
		final long endCount = mongoStorer.count(UIEntity.class, Optional.of(loc));
		log.info(String.format("loaded/updated %s entities in %s to mongo from %s in %s (%s new entities)", 
				entUrls.size(), lang, ImmutableList.copyOf(dbpediaTtl), (System.currentTimeMillis() - start), (endCount - startCount)));
		for (File f: summs.keySet()) {
			log.info("" + f + ":\n\t" + summs.get(f).toString());
			try {
				writeEntityUrisToFile(summs.get(f).missingEntUrls, new File("target/missingEnts/" + f.getName() + "txt"));
			} catch (Exception e) {
				log.error("Error writing missing entities to file", e);
			}
		}
		return summs;
	}

	public static class EntityUpsertFromTTlSummary {
		public EntityUpsertFromTTlSummary(Set<String> set) {
			missingEntUrls = set;
		}
		Set<String> missingEntUrls;
		long upserts = 0;
		long labels = 0;
		long labelsCorrectLang = 0;
		long depictions = 0;
		long types = 0;
		long timeMs = 0;
		@Override
		public String toString() {
			return String
					.format("EntityUpsetFromTTlSummary [timeMs=%s, upserts=%s, labels=%s, labelsCorrectLang=%s, depictions=%s, types=%s, missingEntUrls=%s]",
							timeMs, upserts, labels, labelsCorrectLang, depictions,
							types, missingEntUrls.size());
		}
	}
	
	private EntityUpsertFromTTlSummary updateUIEntitiesInMongo(final Set<String> entUrls, final Locale locale,
			File dbpttl) {
		final long start = System.currentTimeMillis();
		try {
			return Files.readLines(dbpttl, Charsets.UTF_8, new LineProcessor<EntityUpsertFromTTlSummary>(){
				private long currLine = 0;
				private long triplesParsed = 0;
				private long triplesProcessed = 0;
				private UIEntity currEntBuilder;
				private EntityUpsertFromTTlSummary summary = new EntityUpsertFromTTlSummary(new HashSet<>(entUrls));

				@Override
				public EntityUpsertFromTTlSummary getResult() {
					mongoStorer.insertOrUpdate(currEntBuilder, Optional.of(locale));
					summary.upserts++;
					summary.timeMs = System.currentTimeMillis() - start;
					return summary;
				}

				@Override
				public boolean processLine(String line) throws IOException {
					currLine++;
					printProgress();
					if (line.startsWith("#")) return true;
					Optional<Triple> optT = parseTriple(line);
					if (optT.isPresent()) {
						triplesParsed++;
						Triple t = optT.get();
						if (processTriple(t)) triplesProcessed++;
					}
					return true;
				}

				private boolean processTriple(Triple t) {
					try {
						String tripEntUri = t.getSubject().getURI();
						if (entUrls.contains(tripEntUri)) {
							summary.missingEntUrls.remove(tripEntUri);
							if (currEntBuilder == null) {
								currEntBuilder = retrieveOrNew(locale, tripEntUri);
							} else	if (!currEntBuilder.getUrl().equals(tripEntUri)) {
								//finish building currEnt and store in Mongo
								mongoStorer.insertOrUpdate(currEntBuilder, Optional.of(locale));
								summary.upserts++;
								currEntBuilder = retrieveOrNew(locale, tripEntUri);
							}
							appendInfoToCurrent(t);
						} else {
							log.trace("Ignoring " + tripEntUri);
						}
					} catch (Exception e) {
						log.error("Failed to process " + t, e);
						return false;
					}
					return true;
				}

				private UIEntity retrieveOrNew(final Locale locale,
						String tripEntUri) {
					Optional<UIEntity> entInMongo = mongoStorer.findResource(UIEntity.class, Optional.of(locale), tripEntUri);
					UIEntity newUIEnt = entInMongo.isPresent() ? entInMongo.get() : newUIEntity(tripEntUri);
					return newUIEnt;
				}

				private void appendInfoToCurrent(Triple t) {
					String p = t.getPredicate().getURI();
					if (p.equals("http://www.w3.org/2000/01/rdf-schema#label")) {
						summary.labels++;
						try {
							Node o = t.getObject();
							assert(o.isLiteral());
							LiteralLabel lit = o.getLiteral();

							if (o.getLiteralLanguage().equals(locale.getLanguage())) {
								String newVal = (String)o.getLiteral().getValue();
								log.debug(String.format("Updating label from %s to %s", currEntBuilder.getLabel(), newVal));
								summary.labelsCorrectLang++;
								currEntBuilder.setLabel(newVal);
							}
						} catch (Exception e) {
							log.error("Error reading label value", e);
						}
					}
					if (p.equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")) {
						summary.types++;
						try {
							Node o = t.getObject();
							assert(o.isURI());
							String type = o.getURI();
							List<String> types = currEntBuilder.getTypes();
							if (types == null) types = new ArrayList<String>();
							if (!types.contains(type)) types.add(type);
							currEntBuilder.setTypes(types);
						} catch (Exception e) {
							log.error("Error reading type value", e);
						}
					}
					if (p.equals("http://xmlns.com/foaf/0.1/depiction") || p.equals("http://dbpedia.org/ontology/thumbnail")) {
						summary.depictions++;
						try {
							Node o = t.getObject();
							assert(o.isURI());
							String imgUrl = o.getURI();
							List<String> pics = currEntBuilder.getDepictions();
							if (pics == null) pics = new ArrayList<>();
							if (!pics.contains(imgUrl)) pics.add(imgUrl);
							currEntBuilder.setDepictions(pics);
						} catch (Exception e) {
							log.error("Error reading depiction value", e);
						}
					}
				}

				private UIEntity newUIEntity(String entUri) {
					UIEntity result = new UIEntity();
					result.setUrl(entUri);
					return result;
				}

				private void printProgress() {
					if (currLine % 1000 != 0)  return;
					double seconds = (double)(System.currentTimeMillis() - start) / 1000.0;
					System.out.println(String.format("Line %s, triplesRead %s, triplesProcessed %s, speed %s lines/sec", 
							currLine, triplesParsed, triplesProcessed, currLine/seconds));
				}

				private Optional<Triple> parseTriple(String line) {
					try {
						Optional<Dataset> ds = parse(line.getBytes(), Lang.TTL);
						if (ds.isPresent()) {
							Model model = ds.get().getDefaultModel();
							assert(model.size() == 1);
							return Optional.of(model.listStatements().next().asTriple());
						}
						return Optional.absent();
					} catch (Exception e) {
						System.err.println("Failed to parse " + line + ". " + e.getLocalizedMessage());
						return Optional.absent();
					}
				}

				private Optional<Dataset> parse(byte[] bytes, Lang lang) {
					Dataset dataset = DatasetFactory.createMem();
					try {
						InputStream stream = new ByteArrayInputStream(
								bytes);
						RDFDataMgr.read(dataset, stream, lang);
					} catch (Exception e) {
						System.err.println("Failed to parse RDF from input stream. " + e.getLocalizedMessage());
						dataset.close();
						return Optional.absent();
					}
					return Optional.of(dataset);
				}

			});
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}

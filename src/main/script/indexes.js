/*
 * Creates indexes for efficient querying an xLiMe Mongo database.
 * 
 * This is a server-side javascript for managing a Mongo database. 
 * See https://docs.mongodb.com/manual/tutorial/write-scripts-for-the-mongo-shell/
 * To invoke, open a terminal and call
 *   mongo localhost:27017/test indexes.js
 * 
 */


//MicroPostBean

// enable searching of Microposts by creation date, by default show most recent first (i.e. descending)
db.MicroPostBean.createIndex({"created.timestamp":-1});
// enable searching of Microposts by text (use detected language for optimal indexing)
db.MicroPostBean.createIndex({"content.full":"text" }, {language_override:"lang"});


//TVProgramBean

// enable searching of TVPrograms by broadcastDate, by default show most recent first (descending)
db.TVProgramBean.createIndex({"broadcastDate.timestamp":-1});
// enable searching of TVPrograms by description and title texts. Put more weight on the title.
db.TVProgramBean.createIndex({"description.full":"text", title:"text"},{weights:{"title":4}});


//NewsArticleBean

// enable searching of NewsArticles by creation date
db.NewsArticleBean.createIndex({"created.timestamp":-1});
// enable searching of NewsArticles by their content and title texts. put more weight on the title and take detected language into account.
db.NewsArticleBean.createIndex({"content.full":"text", title: "text" },{ weights: { "title": 4 }, language_override: "lang"  }  );


//EntityAnnotation

//entity.url: allows to search resources with the url of the KB entity (for collection with ~2M EntAnns, indexing takes 12s)
db.EntityAnnotation.createIndex({"entity._id":"hashed"});
//resource.url: allows to search entities by the url of the resource that was annotated (e.g. a news article uri or a subtitle track uri)
//  (for collection with ~2M EntAnns, indexing takes 25s)
db.EntityAnnotation.createIndex({"resourceUrl":"hashed"});
// enable searching by insertion and mention dates
db.EntityAnnotation.createIndex({"insertionDate": -1});
db.EntityAnnotation.createIndex({"mentionDate.timestamp": -1});


//SubtitleSegment

//partOf.partOf: allows to search subtitles by the url of a tv program
db.SubtitleSegment.createIndex({"partOf.partOf._id":"hashed"});
//partOf.startTime.timestamp: allows to search by date of the video segment
db.SubtitleSegment.createIndex({"partOf.startTime.timestamp":-1});
//text: allows to search by text
db.SubtitleSegment.createIndex({"text":"text"},{language_override:"lang"});

//OCRAnnotation
// inSegment.partOf.url (allows to search OCRAnnotations by tvProgId)
db.OCRAnnotation.createIndex( { "inSegment.partOf._id" : "hashed" } )
// recognizedText (allows to search by text)
db.OCRAnnotation.createIndex( { "recognizedText": "text" } )
// inSegment.startTime.timestamp: allows to search by date of the video segment
db.OCRAnnotation.createIndex({"inSegment.startTime.timestamp":-1});

//ASRAnnotation
// inSegment.partOf.url (allows to search ASRAnnotations by tvProgId)
db.ASRAnnotation.createIndex( { "inSegment.partOf._id" : "hashed" } )
// recognizedText (allows to search by text)
db.ASRAnnotation.createIndex( { "recognizedText": "text"}, { language_override: "lang" } )
// inSegment.startTime.timestamp: allows to search by date of the video segment
db.ASRAnnotation.createIndex( { "inSegment.startTime.timestamp":-1});

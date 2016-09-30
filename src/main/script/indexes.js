//Adds indexes for querying


//MicroPostBean

//on date: created.timestamp: descending
db.MicroPostBean.createIndex({"created.timestamp":-1});
//on text: content.full
db.MicroPostBean.createIndex({"content.full":"text" }, {language_override:"lang"});


//TVProgramBean

//broadcastDate.timestamp: descending
db.TVProgramBean.createIndex({"broadcastDate.timestamp":-1});
//on text: description.full, title
db.TVProgramBean.createIndex({"description.full":"text", title:"text"},{weights:{"title":4}});


//NewsArticleBean

//create.timestamp: descending
db.NewsArticleBean.createIndex({"created.timestamp":-1});
//on text: content.full, title
db.NewsArticleBean.createIndex({"content.full":"text", title: "text" },{ weights: { "title": 4 }, language_override: "lang"  }  );


//EntityAnnotation

//entity.url: allows to search resources with this entity (for collection with ~2M EntAnns, indexing takes 12s)
db.EntityAnnotation.createIndex({"entity._id":"hashed"});
//resource.url: allows to search entities for this resource (for collection with ~2M EntAnns, indexing takes 25s)
db.EntityAnnotation.createIndex({"resourceUrl":"hashed"});


//SubtitleSegment

//partOf.partOf: allows to search subtitles by tvProgId
db.SubtitleSegment.createIndex({"partOf.partOf._id":"hashed"});
//partOf.startTime.timestamp: allows to search by date
db.SubtitleSegment.createIndex({"partOf.startTime.timestamp":-1});
//text: allows to search by text
db.SubtitleSegment.createIndex({"text":"text"},{language_override:"lang"});
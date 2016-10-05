/**
 * trimMongo.js
 * Server side script for trimming an xLiMe  mongo databse based on the publication dates of media items.
 * 
 * This script is meant to be used in conjunction with some data loader which pushes new data from a 
 * stream such as Kafka; causing the mongo db to grow steadily with new data. To avoid growing too much, this 
 * script can be scheduled to run at fixed times to only keep a certain window of xLiMe data.
 *
 *  
 */

/**
 * Whether to perform the removal of items from mongoDB or just to simulate it 
 * (counting the number of documents to be removed).
 */
var mock = true;

/**
 * The number of minutes to keep (calculated from the most recent time for which we have data 
 * about all three media items).
 */
//var minutesToKeep = 60; //1 hour
var minutesToKeep = 60 * 24 * 7; //7 days

/**
 * Returns the oldest publication date of a media item in the database, or the current date if there 
 * are no media items.
 * E.g. suppose we call this method on 1/6/16 and in the db we have:
 *   - microposts between 1/5/16 and 30/5/16 
 *   - news artic between 3/5/16 and 29/5/16
 *   - tv progs   between 5/5/16 and 10/6/16 
 * This method should return 29/5/16 as its the oldest date between 1/6, 29/5, 30/5 and 10/6.
 * 
 * If we didn't have any microposts and news articles, this method would return 1/6/16 as dates
 * in the future are not allowed.
 */
function findMostRecentSharedDate() {
    var newestMicroPostDate = new Date();
    var newestNewsArtDate = new Date();
    var newestTVDate = new Date();

    var cursor = db.MicroPostBean.find().
        sort( { "created.timestamp" : -1 } ).
        limit(1);
    if (cursor.hasNext()) {
        var microPost = cursor.next();
        newestMicroPostDate = microPost.created.timestamp;
        print("Newest  micropost date: " + newestMicroPostDate);
    }
    
    cursor = db.NewsArticleBean.find().
        sort( { "created.timestamp" : -1 } ).
        limit(1);
    if (cursor.hasNext()) {
        newsArt = cursor.next();
        newestNewsArtDate = newsArt.created.timestamp;
        print("Newest news article date: " + newestNewsArtDate);
        //    printjson( newsArt );
    }
    
    cursor = db.TVProgramBean.find().
        sort( { "broadcastDate.timestamp" : -1 } ).
        limit(1);
    if (cursor.hasNext()) {
        tvProg = cursor.next();
        newestTVDate = tvProg.broadcastDate.timestamp;
        print("Newest tv program date: " + newestTVDate);
        //    printjson( newestNewsArt );
    }
    return minDate(new Date(), //avoid dates in the future
                   minDate(newestTVDate, minDate(newestMicroPostDate, newestNewsArtDate)));
}

function minDate(dateA, dateB) {
    if (dateA.getTime() < dateB.getTime())
        return dateA;
    else return dateB;
}

/**
 * Calculates the cutoff date. Media items older than this date will be removed. This is calculated 
 * by determining the most recent shared date and keeping a window of time (defined by `minutesToKeep`).
 */
function calcCutoffDate() {
    mostRecentShared = findMostRecentSharedDate();
    print("Most recent shared date " + mostRecentShared);
    return plusMinutes(mostRecentShared, -minutesToKeep);
}

function plusMinutes(inputDate, minutes) {
    var result = new Date();
    var minsInMs = 1000 * 60 * minutes;
    result.setTime(inputDate.getTime() + minsInMs);
    return result;
}

function plusDays(inputDate, days) {
    var result = new Date();
    var daysInMs = 1000 * 60 * 60 * 24 * days;
    result.setTime(inputDate.getTime() + daysInMs);
    return result;
}

function microPostsBefore(cutoffDate) {
    return db.MicroPostBean.find( { "created.timestamp" : { $lte : cutoffDate }});
}

function removeMicroPostsBefore(cutoffDate) {
    return db.MicroPostBean.remove( { "created.timestamp" : { $lte : cutoffDate }});
}

function newsArticlesBefore(cutoffDate) {
    return db.NewsArticleBean.find( { "created.timestamp" : { $lte : cutoffDate }});
}

function removeNewsArticlesBefore(cutoffDate) {
    return db.NewsArticleBean.remove( { "created.timestamp" : { $lte : cutoffDate }});
}

function tvProgramsBefore(cutoffDate) {
    return db.TVProgramBean.find( { "broadcastDate.timestamp" : { $lte : cutoffDate }});
}

function removeTVProgramsBefore(cutoffDate) {
    return db.TVProgramBean.remove( { "broadcastDate.timestamp" : { $lte : cutoffDate }});
}

function logItemsToRemove(cutoffDate) {
    cursor = microPostsBefore(cutoffDate);
    print("MicroPosts before " + cutoffDate + ": " + cursor.count());
    cursor = newsArticlesBefore(cutoffDate);
    print("NewsArticles before " + cutoffDate + ": " + cursor.count());
    cursor = tvProgramsBefore(cutoffDate);
    print("TVprogs before " + cutoffDate + ": " + cursor.count());
}


function processAnnBatch(annResult, resUris, annsCursorFn, remAnnsFn) {
//    print("\t updating " + annResult + " with a batch of " + resUris.length + " resource uris");
    annCursor = annsCursorFn(resUris);
    batchCnt = annCursor.count();
    annResult.count = annResult.count + batchCnt;
//    print("\t found " + batchCnt + " annotations in current batch, total:  " + annResult.count);

    var writeResult = remAnnsFn(resUris);
    batchRemCnt = writeResult.nRemoved;
    annResult.removed = annResult.removed + batchRemCnt;
//    print("\t removed " + batchRemCnt + " annotations in current batch, total:  " + annResult.removed);
    if (writeResult.hasWriteConcernError()) {
        wcErr = writeResult.writeConcernError;
        print("\twriteConcernError: " + wcErr);
        annResult.writeConcernErrors.push(wcErr);
    }
    if (writeResult.hasWriteError()) {
        wErr = writeResult.writeError;
        print("\twriteError: " + wErr);
        annResult.writeErrors.push(wErr);
    }
}

function processAnnsFor(mediaItemCursor, annsCursorFn, remAnnsFn, toResourceUriFn) {
    var result = {
        count : 0,
        removed: 0,
        errors: [],
        writeErrors: [],
        writeConcernErrors: []
    };

//    print("Initialised annResult " + result);
    
    var batchSize = 100;
    var logBatchSize = 10000;
    toResourceUriFn = typeof toResourceUriFn !== 'undefined' ? toResourceUriFn : id;
    
    var miUris = [];
    var total = mediaItemCursor.count();
    var cnt = 0;
    try {
        while (mediaItemCursor.hasNext()) {
            var mediaItem = mediaItemCursor.next();
            miUris.push(mediaItem._id);
            cnt++;
            if ((cnt % batchSize) == 0) {
                //print("iterated through " + cnt + " of " + total);
                var resUris = toResourceUriFn(miUris);
                miUris = []; // clean to start next batch
                processAnnBatch(result, resUris, annsCursorFn, remAnnsFn);
            }
            if ((cnt % logBatchSize) == 0) {
                print("iterated through " + cnt + " of " + total);
            }
        }
    } catch (err) {
        result.errors.push(err);
    }
    print("finished iterating through " + cnt + " of " + total);
    var resUris = toResourceUriFn(miUris);
    processAnnBatch(result, resUris, annsCursorFn, remAnnsFn);
    
    print("Found a total of " + result.count + " entity annotations for the given cursor. Of which " + result.removed + " were removed");
    return result;
}


function processEntAnnsFor(mediaItemCursor, toResourceUriFn) {
    return processAnnsFor(mediaItemCursor, entityAnnsForResourceUrls, removeEntityAnnsForResourceUrls, toResourceUriFn);
}

function countEntAnnsFor(mediaItemCursor, toResourceUriFn) {
    return processAnnsFor(mediaItemCursor, entityAnnsForResourceUrls, mockRemove, toResourceUriFn);
}

function processOCRAnnotationsFor(mediaItemCursor) {
    return processAnnsFor(mediaItemCursor, ocrAnnsForResourceUrls, removeOCRAnnsForResourceUrls);
}

function procesASRAnnotationsFor(mediaItemCursor) {
    return processAnnsFor(mediaItemCursor, asrAnnsForResourceUrls, removeASRAnnsForResourceUrls);
}

function countOCRAnnotationsFor(mediaItemCursor) {
    return processAnnsFor(mediaItemCursor, ocrAnnsForResourceUrls, mockRemove);
}

function countASRAnnotationsFor(mediaItemCursor) {
    return processAnnsFor(mediaItemCursor, asrAnnsForResourceUrls, mockRemove);
}

function processSubtitlesToRemove(tvProgCursor) {
    return processAnnsFor(tvProgCursor, subtitlesForTVProgUrls, removeSubtitlesForTVProgUrls);
}

function countSubtitlesToRemove(tvProgCursor) {
    return processAnnsFor(tvProgCursor, subtitlesForTVProgUrls, mockRemove);
}

function asRegexPatterns(uris) {
    var result = [];
    for (var i = 0; i < uris.length; i++) {
        var pattern = "^" + escapeRegExp(uris[i]);
        result.push(new RegExp(pattern));
    }
    return result;
}

function escapeRegExp(string){
  return string.replace(/([.*+?^=!:${}()|\[\]\/\\])/g, "\\$1");
}

function id(uris) {
    return uris;
}

function processAnnotationsToRemove(cutoffDate, isMock) {
    isMock = typeof isMock !== 'undefined' ? isMock : true;

    var result = {};
    
    var cursor = microPostsBefore(cutoffDate);
    print("Removing entAnns for " + cursor.count() + " microposts to delete");
    result.entityAnnotationsForMicroposts = isMock ? countEntAnnsFor(cursor) : processEntAnnsFor(cursor);
    
    cursor = newsArticlesBefore(cutoffDate);
    print("Removing entAnns for " + cursor.count() + " news to delete");
    result.entityAnnotationsForNews = isMock ? countEntAnnsFor(cursor) : processEntAnnsFor(cursor);
    
    cursor = tvProgramsBefore(cutoffDate);
    print("Removing entAnns for " + cursor.count() + " tvprogs to delete");
    result.entityAnnotationsForTVSubtitles = isMock ? countEntAnnsFor(cursor, tvUrisAsSubtitleUris) : processEntAnnsFor(cursor, tvUrisAsSubtitleUris);

/*    cursor = tvProgramsBefore(cutoffDate);
    print("Removing OCR annotations for tvprogs to delete");
    result.ocrAnnotationsForTV = isMock ? countOCRAnnotationsFor(cursor) : processOCRAnnotationsFor(cursor);
*/
    
    return result;
}

function subtitlesForTVProgUrls(tvpUris) {
    return db.SubtitleSegment.find( {_id: { $in: asRegexPatterns(tvpUris) }});
}

function entityAnnsForResourceUrls(resUrls) {
    return db.EntityAnnotation.find( {resourceUrl: { $in: resUrls }});
}

function ocrAnnsForResourceUrls(resUrls) {
    return db.OCRAnnotation.find( { "inSegment.partOf": { $in: resUrls }} );
}

function asrAnnsForResourceUrls(resUrls) {
    return db.ASRAnnotation.find( { "inSegment.partOf": { $in: resUrls }} );
}

function removeSubtitlesForTVProgUrls(tvpUris)  {
    return db.SubtitleSegment.remove( {_id: { $in: asRegexPatterns(tvpUris) }});    
}

function mockRemove(resUrls) {
    return {
        nRemoved: 0,
        hasWriteError: function() { return false; },
        hasWriteConcernError: function() { return false }
    };
}

function removeEntityAnnsForResourceUrls(resUrls) {
    return db.EntityAnnotation.remove( {resourceUrl: { $in: resUrls }} );
}

function removeOCRAnnsForResourceUrls(resUrls) {
    return db.OCRAnnotation.remove( { "inSegment.partOf": { $in: resUrls }}  );
}

function removeASRAnnsForResourceUrls(resUrls) {
    return db.ASRAnnotation.remove( { "inSegment.partOf": { $in: resUrls }} );
}

function tvUrisAsSubtitleUris(tvUris) {
    var result = [];
    for (var i = 0; i < tvUris.length; i++) {
        result.push(tvUris[i] + "/subtitles");
    }
    return result;
}

function processTVPrograms(cutoffDate, isMock) {
    return processMediaItems(cutoffDate, tvProgramsBefore, removeTVProgramsBefore, isMock);
}

function processNewsArticles(cutoffDate, isMock) {
    return processMediaItems(cutoffDate, newsArticlesBefore, removeNewsArticlesBefore, isMock);    
}

function processMicroposts(cutoffDate, isMock) {
    return processMediaItems(cutoffDate, microPostsBefore, removeMicroPostsBefore, isMock);    
}

function processMediaItems(cutoffDate, cursorFn, removeFn, isMock) {
    isMock = typeof isMock !== 'undefined' ? isMock : true;
    var result = {
        count : 0,
        removed: 0,
        writeErrors: [],
        writeConcernErrors: []
    }

    result.count = cursorFn(cutoffDate).count();
    if (!isMock) {
        var writeResult = removeFn(cutoffDate)
        result.removed = writeResult.nRemoved;
        if (writeResult.hasWriteConcernError()) {
            wcErr = writeResult.writeConcernError;
            print("\twriteConcernError: " + wcErr);
            result.writeConcernErrors.push(wcErr);
        }
        if (writeResult.hasWriteError()) {
            wErr = writeResult.writeError;
            print("\twriteError: " + wErr);
            annResult.writeErrors.push(wErr);
        }
    }

    return result;
}

/**
 * Orchestrates the pruning of the mongoDB and returns an object summarising the pruning result.
 * 
 * @param isMock whether to only mock the removal of documents from the mongo db
 * @returns {___anonymous_pruningObj}
 */
function pruneMongoDB(isMock) {
	var result = {};
	result.isMockRun = isMock;
	result.startDate = new Date();
	cutoffDate = calcCutoffDate();
	result.cutoffDate = cutoffDate;
	print("Cutoffdate: " + cutoffDate);
	logItemsToRemove(cutoffDate);
	var tvCursor = tvProgramsBefore(cutoffDate);
	print("Removing Subtitles for " + tvCursor.count() + " tv programs to remove");
	result.subtitleRemoval = mock ? countSubtitlesToRemove(tvCursor) : processSubtitlesToRemove(tvCursor);
	tvCursor = tvProgramsBefore(cutoffDate);
	print("Removing OCR annotations for " + tvCursor.count() + " tv programs");
	result.ocrRemoval = mock ? countOCRAnnotationsFor(tvCursor) : processOCRAnnotationsFor(tvCursor);
	tvCursor = tvProgramsBefore(cutoffDate);
	print("Removing ASR annotations for " + tvCursor.count() + " tv programs");
	result.asrRemoval = mock ? countASRAnnotationsFor(tvCursor) : processASRAnnotationsFor(tvCursor);
	result.annotations = processAnnotationsToRemove(cutoffDate, mock);
	result.tvRemoval = processTVPrograms(cutoffDate, mock);
	result.newsRemoval = processNewsArticles(cutoffDate, mock);
	result.micropostRemoval = processMicroposts(cutoffDate, mock);
	result.endDate = new Date();
	return result;
}

pruningObj = pruneMongoDB(mock);
printjson("Finished mongoDB pruning");
printjson(pruningObj);
try {
	db.Pruning.insertOne(pruningObj);
	print("Saved pruning object to mongo");
} catch (err) {
	print("Failed to save pruning object " + err);
}

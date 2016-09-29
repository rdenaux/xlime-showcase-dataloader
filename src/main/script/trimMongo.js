function findOldestItems() {
    cursor = db.MicroPostBean.find().
        sort( { "created.timestamp" : 1 } ).
        limit(1);
    while (cursor.hasNext()) {
        oldestMicroPost = cursor.next();
        oldestMicroPostDate = oldestMicroPost.created.timestamp;
        print("Oldest micropost date: " + oldestMicroPostDate);
        //    printjson( cursor.next() );
    }
    cursor = db.NewsArticleBean.find().
        sort( { "created.timestamp" : 1 } ).
        limit(1);
    while (cursor.hasNext()) {
        oldestNewsArt = cursor.next();
        oldestNewsArtDate = oldestNewsArt.created.timestamp;
        print("Oldest news article date: " + oldestNewsArtDate);
        //    printjson( oldestNewsArt );
    }
    cursor = db.TVProgramBean.find().
        sort( { "broadcastDate.timestamp" : 1 } ).
        limit(1);
    while (cursor.hasNext()) {
        oldestTVProg = cursor.next();
        oldestTVDate = oldestTVProg.broadcastDate.timestamp;
        print("Oldest tv program date: " + oldestTVDate);
        //    printjson( oldestNewsArt );
    }
}

function findMostRecentSharedDate() {
    cursor = db.MicroPostBean.find().
        sort( { "created.timestamp" : -1 } ).
        limit(1);
    newestMicroPostDate = new Date();
    newestNewsArtDate = new Date();
    newestTVDate = new Date();
    while (cursor.hasNext()) {
        microPost = cursor.next();
        newestMicroPostDate = microPost.created.timestamp;
        print("Newest  micropost date: " + newestMicroPostDate);
    }
    cursor = db.NewsArticleBean.find().
        sort( { "created.timestamp" : -1 } ).
        limit(1);
    while (cursor.hasNext()) {
        newsArt = cursor.next();
        newestNewsArtDate = newsArt.created.timestamp;
        print("Newest news article date: " + newestNewsArtDate);
        //    printjson( newsArt );
    }
    cursor = db.TVProgramBean.find().
        sort( { "broadcastDate.timestamp" : -1 } ).
        limit(1);
    while (cursor.hasNext()) {
        tvProg = cursor.next();
        newestTVDate = tvProg.broadcastDate.timestamp;
        print("Newest tv program date: " + newestTVDate);
        //    printjson( newestNewsArt );
    }
    return minDate(newestTVDate, minDate(newestMicroPostDate, newestNewsArtDate));
}

function minDate(dateA, dateB) {
    if (dateA.getTime() < dateB.getTime())
        return dateA;
    else return dateB;
}

function calcCutoffDate() {
    mostRecentShared = findMostRecentSharedDate();
    print("Most recent shared date " + mostRecentShared);
    return plusDays(mostRecentShared, -7);
}

function plusDays(inputDate, days) {
    result = new Date();
    daysInMs = 1000 * 60 * 60 * 24 * days;
    result.setTime(inputDate.getTime() + daysInMs);
    return result;
}

function logItemsToRemove(cutoffDate) {
    cursor = db.MicroPostBean.find( { "created.timestamp" : { $lte : cutoffDate }});
    print("MicroPosts before " + cutoffDate + ": " + cursor.count());
    cursor = db.NewsArticleBean.find( { "created.timestamp" : { $lte : cutoffDate }});
    print("NewsArticles before " + cutoffDate + ": " + cursor.count());
    cursor = db.TVProgramBean.find( { "broadcastDate.timestamp" : { $lte : cutoffDate }});
    print("TVprogs before " + cutoffDate + ": " + cursor.count());
}

function processEntAnnsFor(mediaItemCursor, toResourceUriFn) {
    batchSize = 100;
    toResourceUriFn = typeof toResourceUriFn !== 'undefined' ? toResourceUriFn : id;
    
    miUris = [];
    total = mediaItemCursor.count();
    cnt = 0;
    eaCnt = 0;
    while (mediaItemCursor.hasNext()) {
        mediaItem = mediaItemCursor.next();
        miUris.push(mediaItem._id);
        cnt++;
        if ((cnt % batchSize) == 0) {
            print("iterated through " + cnt + " of " + total);
            eaCursor = entityAnnsForResourceUrls(toResourceUriFn(miUris));
            batchCnt = eaCursor.count();
            eaCnt = eaCnt + batchCnt;
            print("\t found " + batchCnt + " entityAnns in current batch, total:  " + eaCnt);
            miUris = [];
        }
    }
    eaCursor = entityAnnsForResourceUrls(toResourceUriFn(mpUris));
    batchCnt = eaCursor.count();
    eaCnt = eaCnt + batchCnt;
    
    print("Found a total of " + eaCnt + " entity annotations for the given cursor");
}

function countSubtitlesToRemove(tvProgCursor) {
    batchSize = 100;
    
    tvpUris = [];
    total = tvProgCursor.count();
    cnt = 0;
    stCnt = 0;
    while (tvProgCursor.hasNext()) {
        tvp = tvProgCursor.next();
        tvpUris.push(tvp._id);
        cnt++;
        if ((cnt % batchSize) == 0) {
            print("iterated through " + cnt + " of " + total);
            subCursor = subtitlesForTVProgUrls(tvpUris);
            batchCnt = subCursor.count();
            stCnt = stCnt + batchCnt;
            print("\t found " + batchCnt + " subtitleSegments in current batch, total:  " + stCnt);
            tvpUris = [];
        }
    }
    subCursor = subtitlesForTVProgUrls(tvpUris);
    batchCnt = subCursor.count();
    stCnt = stCnt + batchCnt;
    
    print("Found a total of " + stCnt + " subtitleSegments for the given cursor");
}

function subtitlesForTVProgUrls(tvpUris) {
    return db.SubtitleSegment.find( {_id: { $in: asRegexPatterns(tvpUris) }});
}

function asRegexPatterns(uris) {
    result = [];
    for (var i = 0; i < uris.length; i++) {
        pattern = "^" + escapeRegExp(uris[i]);
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

function countAnnotationsToRemove(cutoffDate) {
    cursor = db.MicroPostBean.find( { "created.timestamp" : { $lte : cutoffDate }});
    print("Counting entAnns for microposts to delete");
    processEntAnnsFor(cursor);
    
    cursor = db.NewsArticleBean.find( { "created.timestamp" : { $lte : cutoffDate }});
    print("Counting entAnns for news to delete");
    processEntAnnsFor(cursor);
    
    cursor = db.TVProgramBean.find( { "broadcastDate.timestamp" : { $lte : cutoffDate }});
    print("Counting entAnns for tvprogs to delete");
    processEntAnnsFor(cursor, tvUrisAsSubtitleUris);
}


function entityAnnsForResourceUrls(resUrls) {
    return db.EntityAnnotation.find( {resourceUrl: { $in: resUrls }});
}

function tvUrisAsSubtitleUris(tvUris) {
    result = [];
    for (var i = 0; i < tvUris.length; i++) {
        result.push(tvUris[i] + "/subtitles");
    }
    return result;
}

cutoffDate = calcCutoffDate();
print("Cutoffdate: " + cutoffDate);
logItemsToRemove(cutoffDate);
tvCursor = db.TVProgramBean.find( { "broadcastDate.timestamp" : { $gt : cutoffDate }});
countSubtitlesToRemove(tvCursor);
//countAnnotationsToRemove(cutoffDate);

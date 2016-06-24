@REM start java app, running a Kafka consumer with configuration file
@echo Starting xLiMe Kafka consumer to push social-media items to Mongo
START "Showcase soc-media to mongo" java -server -Xss128k -Xms128m -Xmx256m -Dlog4j.debug=true -Dlog4j.configuration=file:log4j.xml -cp "lib/*" eu.xlime.kafka.RunExtractor "etc/xlime-kafka-consumer-store-soc-media-to-mongo-config.properties"
@echo Starting xLiMe Kafka consumer to push news articles to Mongo
START "Showcase news to mongo" java -server -Xss128k -Xms128m -Xmx256m -Dlog4j.debug=true -Dlog4j.configuration=file:log4j.xml -cp "lib/*" eu.xlime.kafka.RunExtractor "etc/xlime-kafka-consumer-store-newsfeed-to-mongo-config.properties"
@echo Starting xLiMe Kafka consumer to push tv programs to Mongo
START "Showcase tv-prog to mongo" java -server -Xss128k -Xms128m -Xmx256m -Dlog4j.debug=true -Dlog4j.configuration=file:log4j.xml -cp "lib/*" eu.xlime.kafka.RunExtractor "etc/xlime-kafka-consumer-store-tvprog-to-mongo-config.properties"
@echo Starting xLiMe Kafka consumer to push tv subtitles to Mongo
START "Showcase tv-subs to mongo" java -server -Xss128k -Xms128m -Xmx256m -Dlog4j.debug=true -Dlog4j.configuration=file:log4j.xml -cp "lib/*" eu.xlime.kafka.RunExtractor "etc/xlime-kafka-consumer-store-subtitles-to-mongo-config.properties"
@echo All consumers started
ENDLOCAL

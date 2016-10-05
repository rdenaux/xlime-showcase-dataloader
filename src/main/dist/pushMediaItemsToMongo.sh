#!/bin/sh
# start java app, running a Kafka consumer with configuration file
echo "Starting xLiMe Kafka consumer to push social-media items to Mongo"
java -server -Xss128k -Xms128m -Xmx256m -Dname=socmedia-loader -Dlog4j.debug=true -Dlog4j.configuration=file:log4j.xml -cp "lib/*" eu.xlime.kafka.RunExtractor "etc/xlime-kafka-consumer-store-soc-media-to-mongo-config.properties"
echo "Starting xLiMe Kafka consumer to push news articles to Mongo"
java -server -Xss128k -Xms128m -Xmx256m -Dname=news-loader --Dlog4j.debug=true -Dlog4j.configuration=file:log4j.xml -cp "lib/*" eu.xlime.kafka.RunExtractor "etc/xlime-kafka-consumer-store-newsfeed-to-mongo-config.properties"
echo "Starting xLiMe Kafka consumer to push tv programs to Mongo"
java -server -Xss128k -Xms128m -Xmx256m -Dname=tvprog-loader -Dlog4j.debug=true -Dlog4j.configuration=file:log4j.xml -cp "lib/*" eu.xlime.kafka.RunExtractor "etc/xlime-kafka-consumer-store-tvprog-to-mongo-config.properties"
echo "Starting xLiMe Kafka consumer to push tv subtitles to Mongo"
java -server -Xss128k -Xms128m -Xmx256m -Dname=subtitle-loader -Dlog4j.debug=true -Dlog4j.configuration=file:log4j.xml -cp "lib/*" eu.xlime.kafka.RunExtractor "etc/xlime-kafka-consumer-store-subtitles-to-mongo-config.properties"
echo "Starting xLiMe Kafka consumer to push tv ocr to Mongo"
java -server -Xss128k -Xms128m -Xmx256m -Dname=ocr-loader -Dlog4j.debug=true -Dlog4j.configuration=file:log4j.xml -cp "lib/*" eu.xlime.kafka.RunExtractor "etc/xlime-kafka-consumer-store-tvocr-to-mongo-config.properties"

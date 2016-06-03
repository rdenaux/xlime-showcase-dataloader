#!/bin/sh
# start java app, running a Kafka consumer with configuration file
java -server -Xss256k -Xms512m -Xmx768m -Dlog4j.debug=true -Dlog4j.configuration=file:log4j.xml -cp "lib/*" eu.xlime.kafka.RunExtractor "etc/xlime-kafka-consumer-store-brexit-soc-media-to-mongo-config.properties"

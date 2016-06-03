@REM start java app, running a Kafka consumer with configuration file
@echo Starting xLiMe Kafka consumer to push Brexit social-media items to Mongo
START "" java -server -Xss256k -Xms512m -Xmx768m -Dlog4j.debug=true -Dlog4j.configuration=file:log4j.xml -cp "lib/*" eu.xlime.kafka.RunExtractor "etc/xlime-kafka-consumer-store-brexit-soc-media-to-mongo-config.properties" > "log/console.log"
ENDLOCAL

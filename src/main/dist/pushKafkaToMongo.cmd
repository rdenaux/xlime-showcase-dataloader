@REM start java app, running a Kafka consumer with configuration file
@echo Starting xLiMe Kafka consumers to push various beans to Mongo
START "Showcase all kafka to mongo" java -server -Xss128k -Xms128m -Xmx256m -Dlog4j.debug=true -Dlog4j.configuration=file:log4j.xml -cp "lib/*" eu.xlime.kafka.RunExtractor "etc/xlime-kafka-consumer-store-all-to-mongo-config.properties"
@echo All consumers started
ENDLOCAL

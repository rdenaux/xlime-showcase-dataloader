Utilities for processing xLiMe data into a format suitable for the xLiMe Showcase 

# To execute
  * build the project (`mvn install`) or download release zip
  * unzip the dist file
  * edit the various `properties` files in the `etc/` folder. In particular, point to an xLiMe Kafka instance, 
  	choose a suitable consumer kafka group and point to your mongoDB instance
  * run `pushMediaItemsToMongo.sh` (or `.cmd` depending on your platform)

#Indexing collections
  * set up your mongo environment variables
  * change port and database path in the script if needed
  * run `indexMongoCollections.sh` (or `.cmd` depending on your platform)

# Kafka consumers
## Social-media to MongoDB loader

xLiMe Kafka consumer which:
  * listens to the `socialmedia` xLiMe kafka topic, 
  * parses the kafka messages 
  * extracts the `MicroPosts`, `EntityAnnotation`s and
  * pushes the extracted beans to a MongoDB instance
  
## news-feed to MongoDB loader

xLiMe Kafka consumer which:
  * listens to the `jsi-newsfeed` xLiMe kafka topic, 
  * parses the kafka messages 
  * extracts the `NewsArticleBean`s and `EntityAnnotation`s and
  * pushes the extracted beans to a MongoDB instance

## tvprogram to MongoDB loader

xLiMe Kafka consumer which:
  * listens to the `zattoo-epg` xLiMe kafka topic, 
  * parses the kafka messages 
  * extracts the `TVProgramBean`s
  * pushes the extracted beans to a MongoDB instance

## zattoo-sub consumer which:
  * listens to the `zattoo-sub` xLiMe kafka topic,
  * parses the kafka messages
  * extracts the `SubtitleSegment` and 

Licensed under the Apache Software License, Version 2.0


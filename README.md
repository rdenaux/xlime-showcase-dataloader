Utilities for processing the Brexit dataset for the offline xLiMe Showcase 

# Brexit micropost to MongoDB loader

xLiMe Kafka consumer which:
  * listens to the `socialmedia` xLiMe kafka topic, 
  * parses the kafka messages 
  * extracts those `MicroPosts` which are related to the Brexit referendum
  * pushes the `MicroPostBean`s into a MongoDB instance
  
Note that the annotations are not stored in MongoDB, since we may need to process 
these again using a custom annotator.  

Licensed under the Apache Software License, Version 2.0


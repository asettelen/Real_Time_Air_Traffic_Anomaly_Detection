# There is some docker images which can be
# used ofr every node in th spark cluster


### spark-master ###
image: chasik/sparkmaster:v1
# NB: the container created from this image should necessary have
#"spark-master" as name for the workers to be able to join it 
# docker pull chasik/sparkmaster:v1

### spark-worker ###
image: chasik/sparkworker:v1
# docker pull chasik/sparkworker:v1

### spark-submit ###
image chasik/sparksubmit:v1
# docker pull chasik/sparksubmit:v1


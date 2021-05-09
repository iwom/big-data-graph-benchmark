#!/bin/bash

CLUSTER_NAME_FLINK=flink
CLUSTER_NAME_SPARK=spark
CLUSTER_NAME_GIRAPH=giraph
BUCKET_NAME=dataproc-testing-bucket-iomi
REGION=europe-west3
ZONE=${REGION}-b
IMAGE_VERSION_GIRAPH=1.3-debian10
IMAGE_VERSION_SPARK=2.0-debian10
IMAGE_VERSION_FLINK=2.0-debian10

gcloud beta dataproc clusters create ${CLUSTER_NAME_GIRAPH} --enable-component-gateway --bucket ${BUCKET_NAME} --region ${REGION} --subnet default --zone ${ZONE} --master-machine-type n1-standard-2 --master-boot-disk-size 50 --num-workers 2 --worker-machine-type n1-standard-2 --worker-boot-disk-size 50 --image-version ${IMAGE_VERSION_GIRAPH} --max-age=6h

gcloud beta dataproc clusters create ${CLUSTER_NAME_SPARK} --enable-component-gateway --bucket ${BUCKET_NAME} --region ${REGION} --subnet default --zone ${ZONE} --master-machine-type n1-standard-2 --master-boot-disk-size 50 --num-workers 2 --worker-machine-type n1-standard-2 --worker-boot-disk-size 50 --image-version ${IMAGE_VERSION_SPARK} --max-age=6h

gcloud beta dataproc clusters create ${CLUSTER_NAME_FLINK} --enable-component-gateway --bucket ${BUCKET_NAME} --region ${REGION} --subnet default --zone ${ZONE} --master-machine-type n1-standard-2 --master-boot-disk-size 50 --num-workers 2 --worker-machine-type n1-standard-2 --worker-boot-disk-size 50 --image-version ${IMAGE_VERSION_FLINK} --optional-components FLINK --max-age=6h
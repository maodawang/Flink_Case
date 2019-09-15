
source "$(dirname "$0")"/kafka-common.sh

# prepare Kafka
echo "Preparing Kafka..."

start_kafka_cluster $1 $2 $3
#!/bin/bash
#wget --no-check-certificate https://raw.githubusercontent.com/therson/hdp_utils/master/large_file_download_util.py
#chmod +x large_file_download_util.py
# local download: https://docs.google.com/uc?id=1jMF2Ce8dcZQCHqyhPrlFoJOyLJK7RMzq&export=download

#python large_file_download_util.py 1jMF2Ce8dcZQCHqyhPrlFoJOyLJK7RMzq /tmp/Data-Loader.zip
#unzip /tmp/Data-Loader.zip
#cd /tmp/Data-Loader
#chmod +x *.sh

mkdir /opt/dataloader
chmod 755 /opt/dataloader/ && chown root:root /opt/dataloader/
cd /opt/dataloader
wget -q --no-check-certificate https://s3.eu-west-2.amazonaws.com/whoville/v1/routesHDF32Oct2018.tar.gz
tar -xvf routesHDF32Oct2018.tar.gz
wget -q --no-check-certificate https://s3.eu-west-2.amazonaws.com/whoville/v1/stream-simulator_hdf32Oct2018.jar
wget -q --no-check-certificate https://s3.eu-west-2.amazonaws.com/whoville/v1/smm-consumers_hdf32Oct2018.jar
wget -q --no-check-certificate https://raw.githubusercontent.com/georgevetticaden/sam-trucking-data-utils/hdf-3-2/src/main/resources/nifi-flows/3.2/smm/IOT-Trucking-Fleet-Data-Flow-For-SMM.xml

# Set Logrotation
cat >> /etc/logrotate.conf << EOF
/opt/dataloader/*.log {
    hourly
    rotate 3
    compress
    size 250M
}
EOF

AMBARI_ADMIN_USER=admin
AMBARI_ADMIN_PASSWORD=$(cat /opt/metadata/dps.json |grep -Po 'adminPassword":"[\S]+"'|grep -Po ':"[\S]+"'|grep -Po '[^:"]+')
AMBARI_HOST=$(cat /etc/ambari-agent/conf/ambari-agent.ini| grep -Po 'hostname=[\S]+'|grep -Po '=[\S]+'|grep -Po '[^=]+')
CLUSTER_NAME=$(curl -u $AMBARI_ADMIN_USER:$AMBARI_ADMIN_PASSWORD -X GET http://$AMBARI_HOST:8080/api/v1/clusters |grep cluster_name|grep -Po ': "(.+)'|grep -Po '[a-zA-Z0-9\-_!?.]+')

ZKS1=$(cat /usr/hdp/current/zookeeper-client/conf/zoo.cfg | grep server.1)
[[ ${ZKS1} =~ server.1=(.*?):[0-9]*:[0-9]* ]]
export ZKADDR=${BASH_REMATCH[1]}:2181
echo "using ZooKeeper Server $ZKADDR"


# Loop to create topics
declare -a arr=(
"gateway-west-raw-sensors" "gateway-central-raw-sensors" "gateway-east-raw-sensor" 
"gateway-europe-raw-sensors" "syndicate-geo-event-avro" "syndicate-speed-event-avro"
"syndicate-geo-event-json" "syndicate-speed-event-json" "alerts-speeding-drivers"
"syndicate-oil" "syndicate-battery" "syndicate-transmission" "syndicate-all-geo-critical-events"
"fleet-supply-chain" "route-planning" "load-optimization" "fuel-logistics"
"supply-chain" "predictive-alerts" "energy-mgmt" "audit-events" "compliance"
"adjudication" "approval"
)
echo "Creating Kafka Topics"
for i in "${arr[@]}"
do
   /usr/hdp/current/kafka-broker/bin/kafka-topics.sh \
	--create \
	--zookeeper ${ZKADDR} \
	--replication-factor 1 \
	--partitions 1 \
	--topic "$i"
done

echo "Fetching list of Kafka Brokers"
# Get comma separated string of broker addresses from ZooKeeper
export BROKERIDS=$(/usr/hdp/current/kafka-broker/bin/zookeeper-shell.sh ${ZKADDR} <<< 'ls /brokers/ids' | tail -1)
export BROKERIDS=${BROKERIDS//[!0-9 ]/}
echo "Found Kafka Broker IDS: $BROKERIDS"

unset BROKERS
for i in $BROKERIDS
do
    DETAIL=$(/usr/hdp/current/kafka-broker/bin/zookeeper-shell.sh ${ZKADDR} <<< "get /brokers/ids/$i")
    [[ $DETAIL =~ PLAINTEXT:\/\/(.*?)\"\] ]]
    if [ -z ${BROKERS+x} ]; then BROKERS=${BASH_REMATCH[1]}; else BROKERS="${BROKERS},${BASH_REMATCH[1]}"; fi
done
echo "Found Brokerlist: $BROKERS"

echo "Preparing Producer and Consumer dependencies"
# Producer Dependency Exports
export SIMULATOR_JAR=/opt/dataloader/stream-simulator_hdf32Oct2018.jar
export kafkaBrokers=${BROKERS}
export ROUTES_LOCATION="/opt/dataloader/routes/midwest"
export SECURE_MODE=NONSECURE 
export numOfEuropeTrucks=3
export numOfCriticalEventProducers=5

# Consumer Dependency Exports
export SMM_PRODUCERS_CONSUMERS_SIMULATOR_JAR=/opt/dataloader/smm-consumers_hdf32Oct2018.jar
export schemaRegistryUrl="localhost:7788"
export SRFullUrl="http://${schemaRegistryUrl}/api/v1"
export securityProtocol=PLAINTEXT


# echo "Starting Loading Schemas into Registry"
# java -Xms256m -Xmx2g -cp $SIMULATOR_JAR hortonworks.hdf.sam.refapp.trucking.simulator.schemaregistry.TruckSchemaRegistryLoader $SRFullUrl

# echo "Finished Loading Schemas into Registry"


# START createTruckGenerators.sh
createEuropeTrucks() {
	echo "----------------- Starting International Fleet  ----------------- "
	for ((i=1;i<=numOfEuropeTrucks;i++)); do
	
		clientProducerId='minifi-eu-i'$i
		logFile='eu'$i'.log'
  		echo $clientProducerId
  		waitTime=$((i*2000));
	
		nohup java -Xms256m -Xmx2g -cp \
		$SIMULATOR_JAR \
		hortonworks.hdf.sam.refapp.trucking.simulator.app.smm.SMMSimulationRunnerTruckFleetApp \
		-1 \
		hortonworks.hdf.sam.refapp.trucking.simulator.impl.domain.transport.Truck \
		hortonworks.hdf.sam.refapp.trucking.simulator.impl.collectors.smm.kafka.SMMTruckEventCSVGenerator \
		1 \
		$ROUTES_LOCATION \
		$waitTime \
		$kafkaBrokers \
		ALL_STREAMS \
		$SECURE_MODE \
		$clientProducerId \
		gateway-europe-raw-sensors &> $logFile &
	done
}

createEuropeTrucks
exit 0


createAllGeoCriticalEventProducers() {
	echo "----------------- Starting Geo Event Critical Producers  ----------------- "
	for ((i=1;i<=numOfCriticalEventProducers;i++)); do
	
		clientProducerId='geo-critical-event-collector-i'$i
		logFile='geo-critical-event'$i'.log'
  		echo $clientProducerId
		waitTime=$((i*1000));
		
		nohup java -Xms256m -Xmx2g -cp \
		$SIMULATOR_JAR \
		hortonworks.hdf.sam.refapp.trucking.simulator.app.smm.SMMSimulationRunnerTruckFleetApp \
		-1 \
		hortonworks.hdf.sam.refapp.trucking.simulator.impl.domain.transport.Truck \
		hortonworks.hdf.sam.refapp.trucking.simulator.impl.collectors.smm.kafka.SMMTruckEventCSVGenerator \
		1 \
		$ROUTES_LOCATION \
		$waitTime \
		$kafkaBrokers \
		ALL_STREAMS \
		$SECURE_MODE \
		$clientProducerId \
		syndicate-all-geo-critical-events &> $logFile &
	done
}

createMicroServiceProducers() {

	echo "----------------- Starting Mirco Service Producers  ----------------- "
	topics=(route-planning load-optimization fuel-logistics supply-chain predictive-alerts energy-mgmt audit-events compliance adjudication approval syndicate-oil syndicate-battery syndicate-transmission)
	apps=(route load-optimizer fuel supply-chain predictive energy audit compliance adjudication approval micro-service-oil micro-service-batter micro-service-transmissiony)
	i=0
	for topic in "${topics[@]}"
	do
    	topicName=$topic
        clientProducerId=${apps[i]}-apps
        logFile=$clientProducerId.log;
		waitTime=$((i*2150));
		
		nohup java -Xms256m -Xmx2g -cp \
		$SIMULATOR_JAR \
		hortonworks.hdf.sam.refapp.trucking.simulator.app.smm.SMMSimulationRunnerTruckFleetApp \
		-1 \
		hortonworks.hdf.sam.refapp.trucking.simulator.impl.domain.transport.Truck \
		hortonworks.hdf.sam.refapp.trucking.simulator.impl.collectors.smm.kafka.SMMTruckEventCSVGenerator \
		1 \
		$ROUTES_LOCATION \
		$waitTime \
		$kafkaBrokers \
		ALL_STREAMS \
		$SECURE_MODE \
		$clientProducerId \
		$topicName &> $logFile &        
        
        i=$((i+1))
	done  	
}

createUSFleet() {

echo "----------------- Starting US West Truck Fleet ----------------- "

 nohup java -Xms256m -Xmx2g -cp \
	$SIMULATOR_JAR \
	hortonworks.hdf.sam.refapp.trucking.simulator.app.smm.SMMSimulationRunnerSingleDriverApp \
	-1 \
	hortonworks.hdf.sam.refapp.trucking.simulator.impl.domain.transport.Truck \
	hortonworks.hdf.sam.refapp.trucking.simulator.impl.collectors.smm.kafka.SMMTruckEventCSVGenerator \
	1 \
	$ROUTES_LOCATION \
	5000 \
	$kafkaBrokers \
	ALL_STREAMS \
	$SECURE_MODE \
	minifi-truck-w1 \
	gateway-west-raw-sensors \
	10 \
	"Saint Louis to Tulsa" \
	10 &> w1.log &

 nohup java -Xms256m -Xmx2g -cp \
	$SIMULATOR_JAR \
	hortonworks.hdf.sam.refapp.trucking.simulator.app.smm.SMMSimulationRunnerSingleDriverApp \
	-1 \
	hortonworks.hdf.sam.refapp.trucking.simulator.impl.domain.transport.Truck \
	hortonworks.hdf.sam.refapp.trucking.simulator.impl.collectors.smm.kafka.SMMTruckEventCSVGenerator \
	1 \
	$ROUTES_LOCATION \
	6000 \
	$kafkaBrokers \
	ALL_STREAMS \
	$SECURE_MODE \
	minifi-truck-w2 \
	gateway-west-raw-sensors \
	13 \
	"Des Moines to Chicago" \
	13 &> w2.log &
	
 nohup java -Xms256m -Xmx2g -cp \
	$SIMULATOR_JAR \
	hortonworks.hdf.sam.refapp.trucking.simulator.app.smm.SMMSimulationRunnerSingleDriverApp \
	-1 \
	hortonworks.hdf.sam.refapp.trucking.simulator.impl.domain.transport.Truck \
	hortonworks.hdf.sam.refapp.trucking.simulator.impl.collectors.smm.kafka.SMMTruckEventCSVGenerator \
	1 \
	$ROUTES_LOCATION \
	7000 \
	$kafkaBrokers \
	ALL_STREAMS \
	$SECURE_MODE \
	minifi-truck-w3 \
	gateway-west-raw-sensors \
	14 \
	"Joplin to Kansas City" \
	14 &> w3.log &
	
echo "----------------- Starting US Central Truck Fleet ----------------- "	
	nohup java -Xms256m -Xmx2g -cp \
	$SIMULATOR_JAR \
	hortonworks.hdf.sam.refapp.trucking.simulator.app.smm.SMMSimulationRunnerSingleDriverApp \
	-1 \
	hortonworks.hdf.sam.refapp.trucking.simulator.impl.domain.transport.Truck \
	hortonworks.hdf.sam.refapp.trucking.simulator.impl.collectors.smm.kafka.SMMTruckEventCSVGenerator \
	1 \
	$ROUTES_LOCATION \
	8000 \
	$kafkaBrokers \
	ALL_STREAMS \
	$SECURE_MODE \
	minifi-truck-c1 \
	gateway-central-raw-sensors \
	11 \
	"Saint Louis to Chicago" \
	11 &> c1.log &
	
 nohup java -Xms256m -Xmx2g -cp \
	$SIMULATOR_JAR \
	hortonworks.hdf.sam.refapp.trucking.simulator.app.smm.SMMSimulationRunnerSingleDriverApp \
	-1 \
	hortonworks.hdf.sam.refapp.trucking.simulator.impl.domain.transport.Truck \
	hortonworks.hdf.sam.refapp.trucking.simulator.impl.collectors.smm.kafka.SMMTruckEventCSVGenerator \
	1 \
	$ROUTES_LOCATION \
	9000 \
	$kafkaBrokers \
	ALL_STREAMS \
	$SECURE_MODE \
	minifi-truck-c2 \
	gateway-central-raw-sensors \
	15 \
	"Memphis to Little Rock" \
	15 &> c2.log &
	
 nohup java -Xms256m -Xmx2g -cp \
	$SIMULATOR_JAR \
	hortonworks.hdf.sam.refapp.trucking.simulator.app.smm.SMMSimulationRunnerSingleDriverApp \
	-1 \
	hortonworks.hdf.sam.refapp.trucking.simulator.impl.domain.transport.Truck \
	hortonworks.hdf.sam.refapp.trucking.simulator.impl.collectors.smm.kafka.SMMTruckEventCSVGenerator \
	1 \
	$ROUTES_LOCATION \
	10000 \
	$kafkaBrokers \
	ALL_STREAMS \
	$SECURE_MODE \
	minifi-truck-c3 \
	gateway-central-raw-sensors \
	16 \
	"Peoria to Ceder Rapids" \
	16 &> c3.log &
	
echo "----------------- Starting US East Truck Fleet ----------------- "	

 nohup java -Xms256m -Xmx2g -cp \
	$SIMULATOR_JAR \
	hortonworks.hdf.sam.refapp.trucking.simulator.app.smm.SMMSimulationRunnerSingleDriverApp \
	-1 \
	hortonworks.hdf.sam.refapp.trucking.simulator.impl.domain.transport.Truck \
	hortonworks.hdf.sam.refapp.trucking.simulator.impl.collectors.smm.kafka.SMMTruckEventCSVGenerator \
	1 \
	$ROUTES_LOCATION \
	11000 \
	$kafkaBrokers \
	ALL_STREAMS \
	$SECURE_MODE \
	minifi-truck-e1 \
	gateway-east-raw-sensors \
	12 \
	"Saint Louis to Memphis" \
	12 &> e1.log &	
	
 nohup java -Xms256m -Xmx2g -cp \
	$SIMULATOR_JAR \
	hortonworks.hdf.sam.refapp.trucking.simulator.app.smm.SMMSimulationRunnerSingleDriverApp \
	-1 \
	hortonworks.hdf.sam.refapp.trucking.simulator.impl.domain.transport.Truck \
	hortonworks.hdf.sam.refapp.trucking.simulator.impl.collectors.smm.kafka.SMMTruckEventCSVGenerator \
	1 \
	$ROUTES_LOCATION \
	12000 \
	$kafkaBrokers \
	ALL_STREAMS \
	$SECURE_MODE \
	minifi-truck-e2 \
	gateway-east-raw-sensors \
	17 \
	"Springfield to KC Via Columbia" \
	17 &> e2.log &
	

 nohup java -Xms256m -Xmx2g -cp \
	$SIMULATOR_JAR \
	hortonworks.hdf.sam.refapp.trucking.simulator.app.smm.SMMSimulationRunnerSingleDriverApp \
	-1 \
	hortonworks.hdf.sam.refapp.trucking.simulator.impl.domain.transport.Truck \
	hortonworks.hdf.sam.refapp.trucking.simulator.impl.collectors.smm.kafka.SMMTruckEventCSVGenerator \
	1 \
	$ROUTES_LOCATION \
	13000 \
	$kafkaBrokers \
	ALL_STREAMS \
	$SECURE_MODE \
	minifi-truck-e3 \
	gateway-east-raw-sensors \
	18 \
	"Des Moines to Chicago Route 2" \
	18 &> e3.log &

}

echo "Running Producer Deployment Functions"
createUSFleet;
createEuropeTrucks;
createMicroServiceProducers;
createAllGeoCriticalEventProducers;
echo "Finished Producer Deployment Functions"
# END createTruckGenerators.sh

# START createTruckConsumers.sh

createStringConsumer() {
         nohup java -Xms256m -Xmx2g -cp  \
                $SMM_PRODUCERS_CONSUMERS_SIMULATOR_JAR \
                hortonworks.hdf.smm.refapp.consumer.impl.LoggerStringEventConsumer \
                --bootstrap.servers $kafkaBrokers \
                --schema.registry.url $schemaRegistryUrl \
                --security.protocol $securityProtocol \
                --topics $1 \
                --groupId $2 \
                --clientId $3 \
                --auto.offset.reset latest &>  "$4" &
}

createAvroConsumer() {
         nohup java -Xms256m -Xmx2g -cp  \
                $SMM_PRODUCERS_CONSUMERS_SIMULATOR_JAR \
                hortonworks.hdf.smm.refapp.consumer.impl.LoggerAvroEventConsumer \
                --bootstrap.servers $kafkaBrokers \
                --schema.registry.url $schemaRegistryUrl \
                --security.protocol $securityProtocol \
                --topics $1 \
                --groupId $2 \
                --clientId $3 \
                --auto.offset.reset latest &>  "$4" &
}

createKafkaStreamsConsumerForTruckGeoAvro() {
        topicName="syndicate-geo-event-avro";
        groupId="kafka-streams-analytics-geo-event";
        clientId="consumer-1";
        logFile="kafka-streams-analytics-geo-event.log";
        createAvroConsumer $topicName $groupId $clientId $logFile

}

createSparkStreamingConsumerForTruckGeoAvro() {
        topicName="syndicate-geo-event-avro";
        groupId="spark-streaming-analytics-geo-event";
        clientId="consumer-1";
        logFile="spark-streaming-analytics-geo-event.log";
        createAvroConsumer $topicName $groupId $clientId $logFile

}

createFlinkStreamingConsumerForTruckGeoAvro() {
        topicName="syndicate-geo-event-avro";
        groupId="flink-analytics-geo-event";
        clientId="consumer-1";
        logFile="flink-analytics-geo-event.log";
        createAvroConsumer $topicName $groupId $clientId $logFile

}

createMicroServiceConsumers() {

	
	topics=(route-planning load-optimization fuel-logistics supply-chain predictive-alerts energy-mgmt audit-events compliance adjudication approval)
	services=(route load-optimizer fuel supply-chain predictive energy audit compliance adjudication approval)
	i=0
	for topic in "${topics[@]}"
	do
    	topicName=$topic
        groupId=${services[i]}-micro-service
        clientId=consumer-1;
        logFile=$groupId-$clientId.log;
        createStringConsumer $topicName $groupId $clientId $logFile
        i=$((i+1))
	done  	
	
}

loadSchemas () {
	
	REGISTRY_HOST=$(curl -u $AMBARI_ADMIN_USER:$AMBARI_ADMIN_PASSWORD -X GET http://$AMBARI_HOST:8080/api/v1/clusters/$CLUSTER_NAME/components/REGISTRY_SERVER|grep -Po 'host_name" : "[\S]+'|grep -Po '"[\S]'+|grep -Po '[^"]+')
    
	java -cp stream-simulator_hdf32Oct2018.jar hortonworks.hdf.sam.refapp.trucking.simulator.schemaregistry.TruckSchemaRegistryLoader http://$REGISTRY_HOST:7788/api/v1
}

echo "Loading Schemas into Schema Registry"
loadSchemas
sleep 2

echo "Running Consumer Deployment Functions"
createKafkaStreamsConsumerForTruckGeoAvro
createSparkStreamingConsumerForTruckGeoAvro;
createFlinkStreamingConsumerForTruckGeoAvro;
createMicroServiceConsumers;
echo "Finished Consumer Deployment Functions"
sleep 10
exit 0

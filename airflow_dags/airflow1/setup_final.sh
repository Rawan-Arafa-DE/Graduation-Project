#!/bin/bash

# GitHub Codespaces Big Data Setup Script (Ubuntu) - FIXED VERSION
# Sets up Hadoop, Hive (local mode), Kafka, and Spark in a single-node cluster

set -e  # Exit on any error

echo "🚀 Starting Big Data Environment Setup..."
echo "This will install Hadoop, Hive (local mode), Kafka, and Spark"

# 1. PREPARATORY SETUP (Ubuntu-specific)
echo "1. Installing prerequisites..."
# Update package list
sudo apt-get update

# Install Java 8 for Hive compatibility
sudo apt-get install -y openjdk-8-jdk wget curl nano vim git ssh pdsh openssh-server

# Set Java 8 as default
sudo update-alternatives --set java /usr/lib/jvm/java-8-openjdk-amd64/jre/bin/java
sudo update-alternatives --set javac /usr/lib/jvm/java-8-openjdk-amd64/bin/javac

# Set environment variables for Java 8
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export PATH=$PATH:$JAVA_HOME/bin
echo "export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64" >> ~/.bashrc
echo "export PATH=\$PATH:\$JAVA_HOME/bin" >> ~/.bashrc

# Configure pdsh for Hadoop
echo "export PDSH_RCMD_TYPE=ssh" >> ~/.bashrc
export PDSH_RCMD_TYPE=ssh

# Create base directory
mkdir -p ~/bigdata
cd ~/bigdata

# 2. HADOOP SETUP
echo "2. Installing Hadoop..."
HADOOP_VERSION=3.3.6

# Check if already downloaded
if [ ! -f hadoop-$HADOOP_VERSION.tar.gz ]; then
    wget https://downloads.apache.org/hadoop/common/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz
fi

tar -xzf hadoop-$HADOOP_VERSION.tar.gz
sudo rm -rf /opt/hadoop  # Remove if exists
sudo mv hadoop-$HADOOP_VERSION /opt/hadoop
sudo chown -R $(whoami):$(groups | cut -d' ' -f1) /opt/hadoop

# Set Hadoop environment variables
echo "export HADOOP_HOME=/opt/hadoop" >> ~/.bashrc
echo "export HADOOP_CONF_DIR=\$HADOOP_HOME/etc/hadoop" >> ~/.bashrc
echo "export HADOOP_MAPRED_HOME=\$HADOOP_HOME" >> ~/.bashrc
echo "export HADOOP_COMMON_HOME=\$HADOOP_HOME" >> ~/.bashrc
echo "export HADOOP_HDFS_HOME=\$HADOOP_HOME" >> ~/.bashrc
echo "export YARN_HOME=\$HADOOP_HOME" >> ~/.bashrc
echo "export PATH=\$PATH:\$HADOOP_HOME/bin:\$HADOOP_HOME/sbin" >> ~/.bashrc

export HADOOP_HOME=/opt/hadoop
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export HADOOP_MAPRED_HOME=$HADOOP_HOME
export HADOOP_COMMON_HOME=$HADOOP_HOME
export HADOOP_HDFS_HOME=$HADOOP_HOME
export YARN_HOME=$HADOOP_HOME
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

# Configure Hadoop
mkdir -p $HADOOP_CONF_DIR

# Set JAVA_HOME in hadoop-env.sh
echo "export JAVA_HOME=$JAVA_HOME" >> $HADOOP_CONF_DIR/hadoop-env.sh

cat > $HADOOP_CONF_DIR/core-site.xml << EOF
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://localhost:9000</value>
  </property>
  <property>
    <name>hadoop.tmp.dir</name>
    <value>/opt/hadoop/tmp</value>
  </property>
</configuration>
EOF

cat > $HADOOP_CONF_DIR/hdfs-site.xml << EOF
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>1</value>
  </property>
  <property>
    <name>dfs.namenode.name.dir</name>
    <value>/opt/hadoop/data/namenode</value>
  </property>
  <property>
    <name>dfs.datanode.data.dir</name>
    <value>/opt/hadoop/data/datanode</value>
  </property>
  <property>
    <name>dfs.permissions.enabled</name>
    <value>false</value>
  </property>
</configuration>
EOF

cat > $HADOOP_CONF_DIR/mapred-site.xml << EOF
<configuration>
  <property>
    <name>mapreduce.framework.name</name>
    <value>yarn</value>
  </property>
  <property>
    <name>mapreduce.application.classpath</name>
    <value>\$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/*:\$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/lib/*</value>
  </property>
</configuration>
EOF

cat > $HADOOP_CONF_DIR/yarn-site.xml << EOF
<configuration>
  <property>
    <name>yarn.nodemanager.aux-services</name>
    <value>mapreduce_shuffle</value>
  </property>
  <property>
    <name>yarn.nodemanager.env-whitelist</name>
    <value>JAVA_HOME,HADOOP_COMMON_HOME,HADOOP_HDFS_HOME,HADOOP_CONF_DIR,CLASSPATH_PREPEND_DISTCACHE,HADOOP_YARN_HOME,HADOOP_MAPRED_HOME</value>
  </property>
</configuration>
EOF

# Setup SSH for Hadoop
echo "Setting up SSH..."
# Start SSH service
sudo service ssh start || sudo systemctl start ssh || echo "SSH service start failed, continuing..."

# Generate SSH key (remove old one if exists)
rm -f ~/.ssh/id_rsa ~/.ssh/id_rsa.pub
ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa -q
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 0600 ~/.ssh/authorized_keys

# Configure SSH to not check host key
cat > ~/.ssh/config << EOF
Host localhost
   StrictHostKeyChecking no
   UserKnownHostsFile=/dev/null
Host 0.0.0.0
   StrictHostKeyChecking no
   UserKnownHostsFile=/dev/null
EOF
chmod 600 ~/.ssh/config

# Format HDFS (remove old data if exists)
rm -rf /opt/hadoop/data /opt/hadoop/tmp
mkdir -p /opt/hadoop/data/namenode /opt/hadoop/data/datanode /opt/hadoop/tmp
hdfs namenode -format -force

# 3. HIVE SETUP (LOCAL MODE)
echo "3. Installing Hive (Local Mode)..."
# Use Hive 2.3.9 for better Java 8 compatibility
HIVE_VERSION=2.3.9

if [ ! -f apache-hive-$HIVE_VERSION-bin.tar.gz ]; then
    wget https://archive.apache.org/dist/hive/hive-$HIVE_VERSION/apache-hive-$HIVE_VERSION-bin.tar.gz
fi

tar -xzf apache-hive-$HIVE_VERSION-bin.tar.gz
sudo rm -rf /opt/hive  # Remove if exists
sudo mv apache-hive-$HIVE_VERSION-bin /opt/hive
sudo chown -R $(whoami):$(groups | cut -d' ' -f1) /opt/hive

# Set Hive environment variables
echo "export HIVE_HOME=/opt/hive" >> ~/.bashrc
echo "export PATH=\$PATH:\$HIVE_HOME/bin" >> ~/.bashrc
export HIVE_HOME=/opt/hive
export PATH=$PATH:$HIVE_HOME/bin

# Create local directories for Hive
rm -rf /opt/hive/warehouse /opt/hive/tmp /opt/hive/metastore_db
mkdir -p /opt/hive/warehouse
mkdir -p /opt/hive/tmp
chmod 777 /opt/hive/warehouse
chmod 777 /opt/hive/tmp

# Configure Hive for local mode
mkdir -p $HIVE_HOME/conf

# Set JAVA_HOME in hive-env.sh
cat > $HIVE_HOME/conf/hive-env.sh << EOF
export JAVA_HOME=$JAVA_HOME
export HADOOP_HOME=$HADOOP_HOME
EOF

# Create hive-site.xml for local mode
cat > $HIVE_HOME/conf/hive-site.xml << EOF
<configuration>
  <!-- Metastore configuration -->
  <property>
    <name>javax.jdo.option.ConnectionURL</name>
    <value>jdbc:derby:;databaseName=/opt/hive/metastore_db;create=true</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionDriverName</name>
    <value>org.apache.derby.jdbc.EmbeddedDriver</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionUserName</name>
    <value>APP</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionPassword</name>
    <value>mine</value>
  </property>
  
  <!-- Local file system configuration -->
  <property>
    <name>fs.defaultFS</name>
    <value>file:///</value>
  </property>
  
  <!-- Local warehouse directory -->
  <property>
    <name>hive.metastore.warehouse.dir</name>
    <value>file:///opt/hive/warehouse</value>
  </property>
  
  <!-- Local scratch directory -->
  <property>
    <name>hive.exec.local.scratchdir</name>
    <value>/opt/hive/tmp</value>
  </property>
  
  <!-- Disable schema verification -->
  <property>
    <name>hive.metastore.schema.verification</name>
    <value>false</value>
  </property>
  
  <!-- Auto create schema -->
  <property>
    <name>datanucleus.schema.autoCreateAll</name>
    <value>true</value>
  </property>
  
  <!-- Enable local mode -->
  <property>
    <name>mapreduce.framework.name</name>
    <value>local</value>
  </property>
  
  <!-- Execution engine -->
  <property>
    <name>hive.execution.engine</name>
    <value>mr</value>
  </property>
  
  <!-- Local mode specific settings -->
  <property>
    <name>hive.auto.convert.join</name>
    <value>true</value>
  </property>
  <property>
    <name>hive.auto.convert.join.noconditionaltask.size</name>
    <value>20971520</value>
  </property>
</configuration>
EOF

# Fix Hive logging issue
rm -f $HIVE_HOME/lib/log4j-slf4j-impl-*.jar

# 4. SPARK SETUP
echo "4. Installing Spark..."
# Use Spark 3.3.4 for better compatibility with Java 8
SPARK_VERSION=3.3.4

if [ ! -f spark-$SPARK_VERSION-bin-hadoop3.tgz ]; then
    wget https://archive.apache.org/dist/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop3.tgz
fi

tar -xzf spark-$SPARK_VERSION-bin-hadoop3.tgz
sudo rm -rf /opt/spark  # Remove if exists
sudo mv spark-$SPARK_VERSION-bin-hadoop3 /opt/spark
sudo chown -R $(whoami):$(groups | cut -d' ' -f1) /opt/spark

# Set Spark environment variables
echo "export SPARK_HOME=/opt/spark" >> ~/.bashrc
echo "export PATH=\$PATH:\$SPARK_HOME/bin:\$SPARK_HOME/sbin" >> ~/.bashrc
echo "export PYSPARK_PYTHON=python3" >> ~/.bashrc
export SPARK_HOME=/opt/spark
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
export PYSPARK_PYTHON=python3

# Configure Spark to use Hive (local mode)
cp $HIVE_HOME/conf/hive-site.xml $SPARK_HOME/conf/

# 5. KAFKA SETUP - FIXED VERSION
echo "5. Installing Kafka..."
# Use a stable version that's available
KAFKA_VERSION=3.6.1
SCALA_VERSION=2.13

if [ ! -f kafka_$SCALA_VERSION-$KAFKA_VERSION.tgz ]; then
    # Try to download from Apache archive
    wget https://archive.apache.org/dist/kafka/$KAFKA_VERSION/kafka_$SCALA_VERSION-$KAFKA_VERSION.tgz || \
    wget https://downloads.apache.org/kafka/$KAFKA_VERSION/kafka_$SCALA_VERSION-$KAFKA_VERSION.tgz
fi

tar -xzf kafka_$SCALA_VERSION-$KAFKA_VERSION.tgz
sudo rm -rf /opt/kafka  # Remove if exists
sudo mv kafka_$SCALA_VERSION-$KAFKA_VERSION /opt/kafka
sudo chown -R $(whoami):$(groups | cut -d' ' -f1) /opt/kafka

# Set Kafka environment variable
echo "export KAFKA_HOME=/opt/kafka" >> ~/.bashrc
echo "export PATH=\$PATH:\$KAFKA_HOME/bin" >> ~/.bashrc
export KAFKA_HOME=/opt/kafka
export PATH=$PATH:$KAFKA_HOME/bin

# 6. START SERVICES
echo "6. Starting services..."

# Start Hadoop services (optional - since Hive uses local mode)
echo "Starting Hadoop services (optional for Hive local mode)..."
start-dfs.sh 2>/dev/null || echo "HDFS start failed, continuing..."
start-yarn.sh 2>/dev/null || echo "YARN start failed, continuing..."

# Initialize Hive metastore
echo "Initializing Hive metastore..."
$HIVE_HOME/bin/schematool -dbType derby -initSchema

# Start Kafka (after Zookeeper)
echo "Starting Kafka services..."
$KAFKA_HOME/bin/zookeeper-server-start.sh -daemon $KAFKA_HOME/config/zookeeper.properties
sleep 10  # Wait for Zookeeper
$KAFKA_HOME/bin/kafka-server-start.sh -daemon $KAFKA_HOME/config/server.properties

# Clean up downloaded archives
echo "7. Cleaning up..."
rm -f ~/bigdata/*.tar.gz ~/bigdata/*.tgz

# Create startup scripts
echo "Creating startup scripts..."
mkdir -p ~/scripts

# Create start-hive.sh
cat > ~/scripts/start-hive.sh << 'EOFHIVE'
#!/bin/bash

echo "🐝 Starting Hive Services..."

# Set environment variables
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_HOME=/opt/hadoop
export HIVE_HOME=/opt/hive
export PATH=$PATH:$HIVE_HOME/bin:$HADOOP_HOME/bin

# Check if metastore is initialized
if [ ! -d "/opt/hive/metastore_db" ]; then
    echo "📊 Initializing Hive metastore..."
    $HIVE_HOME/bin/schematool -dbType derby -initSchema
fi

# Create warehouse directory if it doesn't exist
if [ ! -d "/opt/hive/warehouse" ]; then
    echo "📁 Creating Hive warehouse directory..."
    mkdir -p /opt/hive/warehouse
    chmod 777 /opt/hive/warehouse
fi

# Start Hive CLI
echo "✅ Hive is ready! Starting Hive CLI..."
echo ""
echo "📝 Example commands:"
echo "   CREATE TABLE test (id INT, name STRING);"
echo "   INSERT INTO test VALUES (1, 'John'), (2, 'Jane');"
echo "   SELECT * FROM test;"
echo "   SHOW TABLES;"
echo "   EXIT;"
echo ""

# Start Hive
hive
EOFHIVE

# Create start-spark.sh
cat > ~/scripts/start-spark.sh << 'EOFSPARK'
#!/bin/bash

echo "✨ Starting Spark Services..."

# Set environment variables
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export SPARK_HOME=/opt/spark
export HADOOP_HOME=/opt/hadoop
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
export PYSPARK_PYTHON=python3

# Function to show menu
show_menu() {
    echo ""
    echo "Choose Spark interface:"
    echo "1) PySpark (Python)"
    echo "2) Spark-Shell (Scala)"
    echo "3) Spark-SQL"
    echo "4) Start Spark Master & Worker (Standalone Cluster)"
    echo "5) Exit"
    echo ""
}

# Start chosen interface
while true; do
    show_menu
    read -p "Enter your choice [1-5]: " choice
    
    case $choice in
        1)
            echo "✅ Starting PySpark..."
            echo ""
            echo "📝 Example commands:"
            echo "   df = spark.read.text('README.md')"
            echo "   df.show()"
            echo "   spark.sql('SELECT 1 as id, \"Hello\" as greeting').show()"
            echo "   exit()"
            echo ""
            pyspark --master local[*]
            ;;
        2)
            echo "✅ Starting Spark-Shell (Scala)..."
            echo ""
            echo "📝 Example commands:"
            echo "   val df = spark.read.text(\"README.md\")"
            echo "   df.show()"
            echo "   spark.sql(\"SELECT 1 as id, 'Hello' as greeting\").show()"
            echo "   :quit"
            echo ""
            spark-shell --master local[*]
            ;;
        3)
            echo "✅ Starting Spark-SQL..."
            echo ""
            echo "📝 Example commands:"
            echo "   CREATE TABLE test (id INT, name STRING) USING csv;"
            echo "   SELECT 1 as id, 'Hello' as greeting;"
            echo "   SHOW TABLES;"
            echo "   quit;"
            echo ""
            spark-sql --master local[*]
            ;;
        4)
            echo "🚀 Starting Spark Standalone Cluster..."
            $SPARK_HOME/sbin/start-master.sh
            sleep 2
            $SPARK_HOME/sbin/start-worker.sh spark://localhost:7077
            echo "✅ Spark Master UI: http://localhost:8080"
            echo "✅ To stop: run $SPARK_HOME/sbin/stop-all.sh"
            ;;
        5)
            echo "👋 Goodbye!"
            exit 0
            ;;
        *)
            echo "❌ Invalid option. Please try again."
            ;;
    esac
done
EOFSPARK

# Create start-kafka.sh
cat > ~/scripts/start-kafka.sh << 'EOFKAFKA'
#!/bin/bash

echo "📨 Starting Kafka Services..."

# Set environment variables
export KAFKA_HOME=/opt/kafka
export PATH=$PATH:$KAFKA_HOME/bin

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Function to check if service is running
check_service() {
    if pgrep -f "$1" > /dev/null; then
        return 0
    else
        return 1
    fi
}

# Function to start services
start_services() {
    # Start Zookeeper
    if check_service "zookeeper"; then
        echo -e "${GREEN}✓${NC} Zookeeper is already running"
    else
        echo "Starting Zookeeper..."
        $KAFKA_HOME/bin/zookeeper-server-start.sh -daemon $KAFKA_HOME/config/zookeeper.properties
        sleep 5
        if check_service "zookeeper"; then
            echo -e "${GREEN}✅ Zookeeper started successfully${NC}"
        else
            echo -e "${RED}❌ Failed to start Zookeeper${NC}"
            exit 1
        fi
    fi

    # Start Kafka
    if check_service "kafka\.Kafka"; then
        echo -e "${GREEN}✓${NC} Kafka is already running"
    else
        echo "Starting Kafka broker..."
        $KAFKA_HOME/bin/kafka-server-start.sh -daemon $KAFKA_HOME/config/server.properties
        sleep 5
        if check_service "kafka\.Kafka"; then
            echo -e "${GREEN}✅ Kafka broker started successfully${NC}"
        else
            echo -e "${RED}❌ Failed to start Kafka broker${NC}"
            exit 1
        fi
    fi
}

# Function to stop services
stop_services() {
    echo "Stopping Kafka services..."
    $KAFKA_HOME/bin/kafka-server-stop.sh
    sleep 2
    $KAFKA_HOME/bin/zookeeper-server-stop.sh
    sleep 2
    echo "✅ All Kafka services stopped"
}

# Function to show status
show_status() {
    echo ""
    echo "📊 Service Status:"
    if check_service "zookeeper"; then
        echo -e "   Zookeeper: ${GREEN}Running${NC}"
    else
        echo -e "   Zookeeper: ${RED}Stopped${NC}"
    fi
    
    if check_service "kafka\.Kafka"; then
        echo -e "   Kafka: ${GREEN}Running${NC}"
        echo "   Kafka broker: localhost:9092"
    else
        echo -e "   Kafka: ${RED}Stopped${NC}"
    fi
    echo ""
}

# Function to show Kafka commands
show_commands() {
    echo "📝 Useful Kafka commands:"
    echo ""
    echo "# Create a topic:"
    echo "kafka-topics.sh --create --topic test-topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1"
    echo ""
    echo "# List topics:"
    echo "kafka-topics.sh --list --bootstrap-server localhost:9092"
    echo ""
    echo "# Produce messages:"
    echo "kafka-console-producer.sh --topic test-topic --bootstrap-server localhost:9092"
    echo ""
    echo "# Consume messages:"
    echo "kafka-console-consumer.sh --topic test-topic --from-beginning --bootstrap-server localhost:9092"
    echo ""
    echo "# Describe topic:"
    echo "kafka-topics.sh --describe --topic test-topic --bootstrap-server localhost:9092"
    echo ""
}

# Main menu
while true; do
    echo ""
    echo "Kafka Manager Menu:"
    echo "1) Start Kafka services"
    echo "2) Stop Kafka services"
    echo "3) Show status"
    echo "4) Show useful commands"
    echo "5) Create test topic"
    echo "6) Test producer/consumer"
    echo "7) Exit"
    echo ""
    read -p "Enter your choice [1-7]: " choice
    
    case $choice in
        1)
            start_services
            show_status
            ;;
        2)
            stop_services
            ;;
        3)
            show_status
            ;;
        4)
            show_commands
            ;;
        5)
            echo "Creating test topic..."
            kafka-topics.sh --create --topic test-topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 2>/dev/null || echo "Topic might already exist"
            kafka-topics.sh --list --bootstrap-server localhost:9092
            ;;
        6)
            echo "Starting test producer/consumer..."
            echo "Please open a new terminal and run:"
            echo "kafka-console-consumer.sh --topic test-topic --from-beginning --bootstrap-server localhost:9092"
            echo ""
            echo "Type messages below (Ctrl+C to exit):"
            kafka-console-producer.sh --topic test-topic --bootstrap-server localhost:9092
            ;;
        7)
            echo "👋 Goodbye!"
            exit 0
            ;;
        *)
            echo "❌ Invalid option. Please try again."
            ;;
    esac
done
EOFKAFKA

# Make scripts executable
chmod +x ~/scripts/*.sh

# Create a test script for Hive local mode
cat > ~/test_hive_local.sh << 'EOF'
#!/bin/bash
echo "Testing Hive in local mode..."

# Create a test table
hive -e "CREATE TABLE IF NOT EXISTS test_local (id INT, name STRING) STORED AS TEXTFILE;"

# Insert some data
hive -e "INSERT INTO test_local VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie');"

# Query the data
echo "Data in test_local table:"
hive -e "SELECT * FROM test_local;"

# Show tables
echo "Available tables:"
hive -e "SHOW TABLES;"

# Check warehouse directory
echo "Files in local warehouse:"
ls -la /opt/hive/warehouse/
EOF

chmod +x ~/test_hive_local.sh

echo "✅ Setup complete!"
echo ""
echo "📋 Services status:"
echo "   Hive: Ready (local mode - no HDFS required)"
echo "   Spark: Ready"
echo "   Kafka: Running on port 9092"
echo "   HDFS/YARN: Available but not required for Hive local mode"
echo ""
echo "🚀 Quick start scripts created in ~/scripts/:"
echo "   ~/scripts/start-hive.sh  - Start Hive"
echo "   ~/scripts/start-spark.sh - Start Spark (interactive menu)"
echo "   ~/scripts/start-kafka.sh - Start/manage Kafka services"
echo ""
echo "✅ Setup complete! All services should be running."
echo ""
echo "   Hive is now configured to store table data in: ~/hive_warehouse"
echo ""
echo "📋 To check running Java processes, use: jps"
echo ""
echo "   HDFS NameNode UI: http://localhost:9870"
echo "   YARN ResourceManager UI: http://localhost:8088"
echo "   Kafka: Broker available on localhost:9092"
echo ""
echo "🔧 To use components:"
echo "   Hadoop: hdfs dfs -ls /"
echo "   Hive:   hive"
echo "   Spark:  pyspark or spark-shell"
echo "   Kafka:  kafka-topics.sh --list --bootstrap-server localhost:9092"
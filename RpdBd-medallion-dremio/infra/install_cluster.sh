#!/usr/bin/env bash
# instalacja klastra hdfs + spark + kafka (kraft) na n1..n5, uruchamiac na n1
set -euo pipefail

HADOOP_V=3.3.6
SPARK_V=3.5.3
KAFKA_V=3.7.1
SCALA_V=2.13

ALL="n1 n2 n3 n4 n5"
WORKERS="n2 n3 n4 n5"
KAFKA_NODES="n1 n2 n3"

BASE=$HOME/cluster
DL=$HOME/dl
GEN=$HOME/cluster-gen
JH=/usr/lib/jvm/java-17-openjdk-amd64

HADOOP_TGZ=hadoop-$HADOOP_V.tar.gz
SPARK_TGZ=spark-$SPARK_V-bin-hadoop3.tgz
KAFKA_TGZ=kafka_$SCALA_V-$KAFKA_V.tgz

SSH="ssh -o BatchMode=yes"

[ "$(hostname)" = "Bigdata-wn212-06" ] || { echo "uruchom to na n1 (wn212-06)"; exit 1; }
[ -d "$JH" ] || { echo "brak $JH, sprawdz sciezke javy"; exit 1; }

# 1. /etc/hosts na wszystkich wezlach
echo "[1/6] sprawdzam nazwy n1..n5 na wezlach"
cat > /tmp/hosts_add <<EOF
10.7.11.26 n1
10.7.11.27 n2
10.7.11.28 n3
10.7.11.41 n4
10.7.11.42 n5
EOF
PW=""
for n in $ALL; do
  missing=$($SSH $n 'c=0; for h in n1 n2 n3 n4 n5; do getent hosts $h >/dev/null || c=$((c+1)); done; echo $c')
  if [ "$missing" != "0" ]; then
    echo "  $n: brakuje wpisow, dopisuje"
    if [ -z "$PW" ]; then
      read -r -s -p "  haslo sudo (nigdzie nie jest zapisywane): " PW; echo
    fi
    scp -q /tmp/hosts_add $n:/tmp/hosts_add
    printf '%s\n' "$PW" | $SSH $n "sudo -S -p '' bash -c 'while read -r ip name; do grep -qw \$name /etc/hosts || echo \"\$ip \$name\" >> /etc/hosts; done < /tmp/hosts_add'"
  fi
done
PW=""

# 2. pobranie paczek (raz, na n1)
echo "[2/6] pobieram paczki na n1"
mkdir -p $DL
get() {
  local url=$1 f=$2
  if [ ! -s "$DL/$f" ]; then
    curl -fL --progress-bar -o "$DL/$f.part" "$url"
    mv "$DL/$f.part" "$DL/$f"
  fi
  gzip -t "$DL/$f"
}
get "https://archive.apache.org/dist/hadoop/common/hadoop-$HADOOP_V/$HADOOP_TGZ" $HADOOP_TGZ
get "https://archive.apache.org/dist/spark/spark-$SPARK_V/$SPARK_TGZ" $SPARK_TGZ
get "https://archive.apache.org/dist/kafka/$KAFKA_V/$KAFKA_TGZ" $KAFKA_TGZ

# 3. kopiowanie i rozpakowanie
echo "[3/6] kopiuje i rozpakowuje na wezlach"
for n in $ALL; do
  echo "  $n"
  $SSH $n "mkdir -p $DL $BASE/data"
  files="$HADOOP_TGZ $SPARK_TGZ"
  case " $KAFKA_NODES " in *" $n "*) files="$files $KAFKA_TGZ";; esac
  for f in $files; do
    [ "$n" = n1 ] || $SSH $n "[ -s $DL/$f ]" || scp -q "$DL/$f" "$n:$DL/"
  done
  $SSH $n "set -e; cd $BASE
    [ -d hadoop-$HADOOP_V ] || tar xzf $DL/$HADOOP_TGZ
    [ -d spark-$SPARK_V-bin-hadoop3 ] || tar xzf $DL/$SPARK_TGZ
    ln -sfn hadoop-$HADOOP_V hadoop
    ln -sfn spark-$SPARK_V-bin-hadoop3 spark"
  case " $KAFKA_NODES " in *" $n "*)
    $SSH $n "set -e; cd $BASE
      [ -d kafka_$SCALA_V-$KAFKA_V ] || tar xzf $DL/$KAFKA_TGZ
      ln -sfn kafka_$SCALA_V-$KAFKA_V kafka";;
  esac
done

# 4. konfiguracja hadoop + spark
echo "[4/6] generuje i wysylam konfiguracje"
rm -rf $GEN; mkdir -p $GEN

cat > $GEN/core-site.xml <<EOF
<?xml version="1.0"?>
<configuration>
  <property><name>fs.defaultFS</name><value>hdfs://n1:9000</value></property>
</configuration>
EOF

cat > $GEN/hdfs-site.xml <<EOF
<?xml version="1.0"?>
<configuration>
  <property><name>dfs.replication</name><value>3</value></property>
  <property><name>dfs.namenode.name.dir</name><value>file://$BASE/data/hdfs/nn</value></property>
  <property><name>dfs.datanode.data.dir</name><value>file://$BASE/data/hdfs/dn</value></property>
  <property><name>dfs.namenode.secondary.http-address</name><value>n1:9868</value></property>
</configuration>
EOF

printf '%s\n' $WORKERS > $GEN/workers

cat > $GEN/spark-env.sh <<EOF
export JAVA_HOME=$JH
export SPARK_MASTER_HOST=n1
export HADOOP_CONF_DIR=$BASE/hadoop/etc/hadoop
export SPARK_WORKER_CORES=6
export SPARK_WORKER_MEMORY=32g
EOF

cat > $GEN/spark-defaults.conf <<EOF
spark.master spark://n1:7077
EOF

cat > $GEN/envblock <<EOF
# >>> cluster >>>
export JAVA_HOME=$JH
export HADOOP_HOME=$BASE/hadoop
export SPARK_HOME=$BASE/spark
export KAFKA_HOME=$BASE/kafka
export HADOOP_CONF_DIR=\$HADOOP_HOME/etc/hadoop
export PATH=\$PATH:\$HADOOP_HOME/bin:\$SPARK_HOME/bin:\$KAFKA_HOME/bin
# <<< cluster <<<
EOF

H=$BASE/hadoop/etc/hadoop
for n in $ALL; do
  scp -q $GEN/core-site.xml $GEN/hdfs-site.xml $GEN/workers $n:$H/
  $SSH $n "grep -q '^export JAVA_HOME=' $H/hadoop-env.sh || echo 'export JAVA_HOME=$JH' >> $H/hadoop-env.sh"
  scp -q $GEN/spark-env.sh $GEN/spark-defaults.conf $GEN/workers $n:$BASE/spark/conf/
  $SSH $n "mkdir -p $BASE/data/hdfs/nn $BASE/data/hdfs/dn"
  scp -q $GEN/envblock $n:/tmp/envblock
  $SSH $n "grep -q '>>> cluster >>>' $HOME/.bashrc || cat /tmp/envblock >> $HOME/.bashrc"
done

# 5. kafka (kraft, 3 brokery = 3 kontrolery)
echo "[5/6] konfiguruje kafke"
[ -s $BASE/kafka_cluster_id ] || $BASE/kafka/bin/kafka-storage.sh random-uuid > $BASE/kafka_cluster_id
CID=$(cat $BASE/kafka_cluster_id)
id=1
for n in $KAFKA_NODES; do
  cat > $GEN/cluster.properties <<EOF
process.roles=broker,controller
node.id=$id
controller.quorum.voters=1@n1:9093,2@n2:9093,3@n3:9093
listeners=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
advertised.listeners=PLAINTEXT://$n:9092
inter.broker.listener.name=PLAINTEXT
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
log.dirs=$BASE/data/kafka
num.partitions=3
default.replication.factor=3
min.insync.replicas=2
offsets.topic.replication.factor=3
transaction.state.log.replication.factor=3
transaction.state.log.min.isr=2
group.initial.rebalance.delay.ms=0
log.retention.hours=72
EOF
  scp -q $GEN/cluster.properties $n:$BASE/kafka/config/
  $SSH $n "mkdir -p $BASE/data/kafka; [ -f $BASE/data/kafka/meta.properties ] || $BASE/kafka/bin/kafka-storage.sh format -t $CID -c $BASE/kafka/config/cluster.properties"
  id=$((id+1))
done

echo "[6/6] gotowe. nic nie jest jeszcze uruchomione."
echo "dalej: source ~/.bashrc, potem formatowanie namenode i start uslug (patrz instrukcja)"

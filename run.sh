JAVA="java"
MAX_MEM="4g"
# 4g is faster, but means I need to shut everything down for a test run
JAVA_FLAGS="-d64"
LOGS_DIR="logs/"


target_jar="-jar target/hft-parser-1.0-SNAPSHOT-jar-with-dependencies.jar"
in_book="-b $1"
out_file="-o $2"
config_file="-c $3"

out_logfile="$LOGS_DIR/$(date +%s).txt"

cmd_to_run="$JAVA -Xms$MAX_MEM -Xmx$MAX_MEM $JAVA_FLAGS $target_jar $in_book $out_file $config_file"

echo $cmd_to_run > $out_logfile
cat ${3} >> $out_logfile
echo "Comment: $4" >> $out_logfile
$cmd_to_run 2>&1 | tee -a $out_logfile

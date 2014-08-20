tar -czvf project.tar.gz normal_run.sh run.sh logs config-files target/hft-parser-1.0-SNAPSHOT-jar-with-dependencies.jar
scp project.tar.gz $1
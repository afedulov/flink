#!/bin/bash

check_one_per_package() {
    read foo
	if [ $foo -gt 1 ]
	then
		echo "ERROR - CHECK FAILED: $1 is shaded multiple times!"
		exit 1
	else
		echo "OK"
	fi
}

check_relocated() {
    read foo
	if [ $foo -ne 0 ]
	then
		echo "ERROR - CHECK FAILED: found $1 classses that where not relocated!"
		exit 1
	else
		echo "OK"
	fi
}

check_one_per_package_file_connector_base() {
  echo "Checking that flink-connector-base is included only once:"
  echo "__________________________________________________________________________"

  CONNECTOR_JARS=$(find flink-connectors -type f -name '*.jar' | grep -vE "original");
  EXIT_CODE=0

  for i in $CONNECTOR_JARS; 
    do
      echo "=== $i: ===";
      jar tf $i | grep 'AWSConfigConstants';
      EXIT_CODE=$((EXIT_CODE+$?))
      echo "__________________________________________________________________________"
    done;
    return $EXIT_CODE;
}

# check_relocated_file_connector_base() {
#   echo -e "\n\n"
#   echo "Checking that flink-connector-base is relocated:"
#   echo "__________________________________________________________________________"

#   CONNECTOR_JARS=$(find flink-connectors -type f -name '*.jar' | \
#     grep -v original | grep -v '\-test' | grep -v 'flink-connectors/flink-connector-base');

#   EXIT_CODE=0
#   for i in $CONNECTOR_JARS;
#     do
#       echo -n "- $i: ";
#       jar tf $i | grep '^org/apache/flink/connector/base/source/reader/RecordEmitter' | wc -l | check_relocated "flink-connector-base";
#       EXIT_CODE=$((EXIT_CODE+$?))
#     done;
#   return $EXIT_CODE;
# }

check_one_per_package_file_connector_base
#check_relocated_file_connector_base

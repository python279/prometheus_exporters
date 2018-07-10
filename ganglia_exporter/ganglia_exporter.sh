#!/bin/sh

cd `dirname $0`

python3 ganglia_exporter.py http://pushgateway/metrics/job/xxx_job/instance/xxx_instance
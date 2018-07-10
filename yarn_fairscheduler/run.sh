#!/bin/sh

cd `dirname $0`

python3 yarn_fairscheduler_exporter http://pushgateway/metrics/job/xxx_job/instance/xxx_instance

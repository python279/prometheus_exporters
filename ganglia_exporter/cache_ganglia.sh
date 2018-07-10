#!/bin/sh

cd `dirname $0`

mkdir xmlcache logs > /dev/null 2>&1
/opt/anaconda2/bin/python ganglia_parser.py xmlcache logs
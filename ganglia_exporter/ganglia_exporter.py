#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import requests_xml
import json
import re
import requests
import glob
import time
import stat
from concurrent.futures import ThreadPoolExecutor


class ganglia_parser():
    def __init__(self, xmlfile):
        self.host = os.path.basename(xmlfile)
        self.xmlfile = xmlfile
        self.output = ''
        self.stale_time = 300

    def parse(self):
        if self.output:
            self.output = ''
        mod_time = os.stat(self.xmlfile)[stat.ST_MTIME]
        if time.time () - mod_time > self.stale_time:
            return self.output
        with open(self.xmlfile) as f:
            doc = re.sub(r'<?.*(encoding=\"UTF-8\"|encoding=\'UTF-8\').*?>', '', f.read(), flags=re.I)
            j = json.loads(requests_xml.XML(xml=doc).json())
            if time.time() - j['HOST']['@REPORTED'] > self.stale_time:
                return self.output
            else:
                for metric in j['HOST']['METRIC']:
                    if metric['@TYPE'] == 'string':
                        continue
                    if isinstance(metric['EXTRA_DATA']['EXTRA_ELEMENT'], list):
                        for e in metric['EXTRA_DATA']['EXTRA_ELEMENT']:
                            if e['@NAME'] == 'GROUP':
                                group = e['@VAL']
                    else:
                        group = metric['EXTRA_DATA']['EXTRA_ELEMENT']['@VAL']
                    try:
                        self.output += '%s{host="%s",group="%s"} %d\n' % (
                            metric['@NAME'].replace('.', '_').replace(' ', '').replace('-', '_'),
                            self.host, group.replace('.', '_').replace(' ', '').replace('-', '_'),
                            metric['@VAL'])
                    except TypeError:
                        self.output += '%s{host="%s",group="%s"} %s\n' % (
                            metric['@NAME'].replace('.', '_').replace(' ', '').replace('-', '_'),
                            self.host, group.replace('.', '_').replace(' ', '').replace('-', '_'),
                            metric['@VAL'])
                return self.output


def quick_parse(xmlfiles, n=100):
    def _ganglia_parser(xmlfile):
        return ganglia_parser(xmlfile).parse()

    with ThreadPoolExecutor(max_workers=n) as executor:
        return '\n'.join(executor.map(_ganglia_parser, xmlfiles))


if __name__ == '__main__':
    pushgateway = sys.argv[1]
    r = requests.put(pushgateway, data=quick_parse(glob.glob(os.path.join('xmlcache', 'hosts', '*'))))
    print(r.content)

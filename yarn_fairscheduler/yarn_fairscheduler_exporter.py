#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import requests_xml
import json
import re
import requests


def get_resourcemanager_webapp():
    yarn_site_xml="/etc/hadoop/conf/yarn-site.xml"
    with open(yarn_site_xml) as f:
        doc = re.sub(r'<?.*(encoding=\"UTF-8\"|encoding=\'UTF-8\').*?>', '', f.read(), flags=re.I)
        j = json.loads(requests_xml.XML(xml=doc).json())
        return [p['value']['$'] for p in j['configuration']['property'] if 'yarn.resourcemanager.webapp.address' in p['name']['$']]


def get_fairscheduler_metrics():
    class QueueParser(object):
        def __init__(self):
            self._queue_metrics = {}

        def parse_queue(self, root):
            name = root['queueName']
            self._queue_metrics[name] = {}
            self._queue_metrics[name]['maxApps'] = root['maxApps']
            self._queue_metrics[name]['minResources'] = root['minResources']
            self._queue_metrics[name]['maxResources'] = root['maxResources']
            self._queue_metrics[name]['usedResources'] = root['usedResources']
            self._queue_metrics[name]['steadyFairResources'] = root['steadyFairResources']
            self._queue_metrics[name]['fairResources'] = root['fairResources']
            self._queue_metrics[name]['clusterResources'] = root['clusterResources']
            if 'numPendingApps' in root.keys():
                self._queue_metrics[name]['numPendingApps'] = root['numPendingApps']
            if 'numActiveApps' in root.keys():
                self._queue_metrics[name]['numActiveApps'] = root['numActiveApps']
            if 'childQueues' in root.keys():
                for q in root['childQueues']:
                    self.parse_queue(q)
            return self

    for h in get_resourcemanager_webapp():
        r = requests.get('http://%s/ws/v1/cluster/scheduler' % h)
        if r.status_code == 200:
            jmx = r.json()
            return QueueParser().parse_queue(jmx['scheduler']['schedulerInfo']['rootQueue'])._queue_metrics


def generate_prometheus_metrics(metrics):
    metrics_text = ''
    metrics_text += '# TYPE version counter\n'
    metrics_text += '# TYPE max_apps counter\n'
    metrics_text += '# TYPE min_resources_memory counter\n'
    metrics_text += '# TYPE min_resources_vcores counter\n'
    metrics_text += '# TYPE max_resources_memory counter\n'
    metrics_text += '# TYPE max_resources_vcores counter\n'
    metrics_text += '# TYPE used_resources_memory counter\n'
    metrics_text += '# TYPE used_resources_vcores counter\n'
    metrics_text += '# TYPE fair_resources_memory counter\n'
    metrics_text += '# TYPE fair_resources_vcores counter\n'
    metrics_text += '# TYPE steady_fair_resources_memory counter\n'
    metrics_text += '# TYPE steady_fair_resources_vcores counter\n'
    metrics_text += '# TYPE num_pending_apps counter\n'
    metrics_text += '# TYPE num_active_apps counter\n'
    metrics_text += 'version 1\n'
    queue_name=[]
    for k, v in metrics.items():
        metrics_text += 'max_apps{queue="%s"} %d\n' %(k, v['maxApps'])
        metrics_text += 'min_resources_memory{queue="%s"} %d\n' %(k, v['minResources']['memory'])
        metrics_text += 'min_resources_vcores{queue="%s"} %d\n' %(k, v['minResources']['vCores'])
        metrics_text += 'max_resources_memory{queue="%s"} %d\n' %(k, v['maxResources']['memory'])
        metrics_text += 'max_resources_vcores{queue="%s"} %d\n' %(k, v['maxResources']['vCores'])
        metrics_text += 'used_resources_memory{queue="%s"} %d\n' %(k, v['usedResources']['memory'])
        metrics_text += 'used_resources_vcores{queue="%s"} %d\n' %(k, v['usedResources']['vCores'])
        metrics_text += 'fair_resources_memory{queue="%s"} %d\n' %(k, v['fairResources']['memory'])
        metrics_text += 'fair_resources_vcores{queue="%s"} %d\n' %(k, v['fairResources']['vCores'])
        metrics_text += 'steady_fair_resources_memory{queue="%s"} %d\n' %(k, v['steadyFairResources']['memory'])
        metrics_text += 'steady_fair_resources_vcores{queue="%s"} %d\n' %(k, v['steadyFairResources']['vCores'])
        if 'numPendingApps' in v.keys():
            metrics_text += 'num_pending_apps{queue="%s"} %d\n' %(k, v['numPendingApps'])
        if 'numActiveApps' in v.keys():
            metrics_text += 'num_active_apps{queue="%s"} %d\n' %(k, v['numActiveApps'])
    return metrics_text


if __name__ == '__main__':
    pushgateway = sys.argv[1]
    r = requests.put(pushgateway, data=generate_prometheus_metrics(get_fairscheduler_metrics()))
    print(r.content)


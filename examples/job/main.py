#!/usr/bin/env python

from datetime import datetime
from dateutil.tz import tzutc
from dateutil.relativedelta import relativedelta

from kontroller import BaseController, log, oid
from kubernetes import client, watch
from kubernetes.config.incluster_config import InClusterConfigLoader

import urllib3
urllib3.disable_warnings()

class JobPrunner(BaseController):

    def start(self, deadline=3600):
        self.core_v1 = client.CoreV1Api()
        self.batch_v1 = client.BatchV1Api()
        self.deadline = deadline
        self.create_watcher(self.batch_v1.list_job_for_all_namespaces)
        self.start_controller(60 * 5)


    def process_objects(self, objs, booting=False):
        if booting:
            for uid, o in objs.items():
                log('>>>', oid(o))
            return

        busy = False
        log('Processing {} jobs'.format(len(objs)))

        for uid, o in objs.items():
            if self.expired(o):
                log('Expired', oid(o), o.metadata.creation_timestamp)
                try:
                    self.delete_job_and_pods(o)
                except client.rest.ApiException as ex:
                    if ex.status != 404:
                        raise
                    log('{} {}: {}'.format(ex.status, ex.reason, ex))
                busy = True
        return busy


    def expired(self, o):
        ct = o.metadata.creation_timestamp
        utcnow_aware = datetime.utcnow().replace(tzinfo=tzutc())
        utc_limit = utcnow_aware - relativedelta(seconds=self.deadline)
        return ct and utc_limit > ct


    def delete_job(self, o):
        md = o.metadata
        log('Deleting {kind} {namespace}/{name}'.format(kind=o.kind, namespace=md.namespace, name=md.name))
        body = client.V1DeleteOptions()
        self.batch_v1.delete_namespaced_job(md.name, md.namespace, body)


    def delete_pod(self, o):
        md = o.metadata
        log('Deleting {kind} {namespace}/{name}'.format(kind=o.kind, namespace=md.namespace, name=md.name))
        body = client.V1DeleteOptions()
        self.core_v1.delete_namespaced_pod(md.name, md.namespace, body)


    ## TODO: async
    def delete_job_and_pods(self, o):
        md = o.metadata
        for p in self.list_controlled_pods(o):
            self.delete_object(p, self.delete_pod)
        self.delete_object(o, self.delete_job)


    def list_controlled_pods(self, o):
        md = o.metadata
        pod_list = self.core_v1.list_namespaced_pod(namespace=md.namespace, label_selector='controller-uid=' + md.uid)
        kind = pod_list.kind[:-4]
        for p in pod_list.items:
            p.kind = kind
            yield p


    def added_object(self, o):
        log('++>', oid(o), o.metadata.creation_timestamp)


    def modified_object(self, old, new):
        log('xx>', oid(old), '-->', oid(new), '@', new.metadata.creation_timestamp)


    def deleted_object(self, o):
        log('-->', oid(o), o.metadata.creation_timestamp)



def main():
    log('Started JobPrunner')

    import os
    deadline_hours = int(os.environ.get('DEADLINE_HOURS', 24))
    log('Job deadline {}h'.format(deadline_hours))

    if 'SERVICE_TOKEN_FILENAME' in os.environ:
        InClusterConfigLoader(
                token_filename=os.environ.get('SERVICE_TOKEN_FILENAME'),
                cert_filename=os.environ.get('SERVICE_CERT_FILENAME')).load_and_set()
    else:
        config.load_incluster_config()

    client.configuration.verify_ssl = False

    jp = JobPrunner(watch=watch.Watch())
    jp.start(deadline_hours * 60 * 60)


if __name__ == '__main__':
    main()

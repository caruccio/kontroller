#!/usr/bin/env python

from kontroller import BaseController, client, log, oid
from kubernetes.config.incluster_config import InClusterConfigLoader

import urllib3
urllib3.disable_warnings()

class NamespaceWatcher(BaseController):

    def start(self):
        self.core_v1 = client.CoreV1Api()
        self.create_watcher(self.core_v1.list_namespace)
        self.create_watcher(self.core_v1.list_pod_for_all_namespaces)
        self.start_controller()


    def process_objects(self, objs, booting=False):
        if booting:
            for uid, o in objs.items():
                log('>>>', oid(o))
        else:
            log('>>> found %i objects' % len(objs))


    def added_object(self, o):
        log('++>', oid(o), o.metadata.creation_timestamp)


    def modified_object(self, old, new):
        log('xx>', oid(old), '-->', oid(new), '@', new.metadata.creation_timestamp)


    def deleted_object(self, o):
        log('-->', oid(o), o.metadata.creation_timestamp)

def main():
    log('Started NamespaceWatcher')

    import os

    if 'SERVICE_TOKEN_FILENAME' in os.environ:
        InClusterConfigLoader(
                token_filename=os.environ.get('SERVICE_TOKEN_FILENAME'),
                cert_filename=os.environ.get('SERVICE_CERT_FILENAME')).load_and_set()
    else:
        config.load_incluster_config()

    client.configuration.verify_ssl = False

    nw = NamespaceWatcher()
    nw.start()


if __name__ == '__main__':
    main()

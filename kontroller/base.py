import os, sys, time
import traceback
from functools import partial

from datetime import datetime

from threading import Thread, Lock, get_ident
from queue import Queue, Empty


def log(*msgs):
    print('[{}] {}'.format(str(datetime.utcnow()), ' '.join([ str(m) for m in msgs ])))


def oid(o):
    if not o:
        return str(None)
    if o.kind == 'Namespace':
        return '{metadata.uid} {kind}/{metadata.name}'.format(metadata=o.metadata, kind=o.kind)
    elif o.kind:
        return '{metadata.uid} {kind}/{metadata.namespace}/{metadata.name}'.format(metadata=o.metadata, kind=o.kind)
    return '{metadata.uid} {metadata.namespace}/{metadata.name}'.format(metadata=o.metadata)


def dt2ts(dt):
    '''datetime -> timestamp'''
    return int((dt - datetime(1970, 1, 1, tzinfo=dt.tzinfo)).total_seconds())


class BaseController(object):
    def __init__(self, watch):
        try:
            self.w = watch.Watch()
        except AttributeError:
            self.w = watch
        self.rv = 0           ## latest seen resourceVersion
        self.c = dict()       ## object cache
        self.q = Queue()      ## event queue
        self.l = Lock()       ## q's lock
        self.dq = Queue()     ## delete queue
        self.dt = Thread(target=self._delete_worker, daemon=True)
        self.dt.start()


    def create_watcher(self, list_func, *vargs, **kwargs):
        self.evt = Thread(target=partial(self._update_worker, list_func, *vargs, **kwargs), daemon=True)
        self.evt.start()


    # main loop
    def start_controller(self, loop_frequency=30):
        log('Loop frequency {}s'.format(loop_frequency))
        mark = dt2ts(datetime.utcnow())

        while True:
            try:
                ev = self.q.get(block=True, timeout=loop_frequency)
                self._process_event(ev)
                self.q.task_done()

                #self.dq.get(block=False)
            except Empty:
                if not self._process_objects(booting=False):
                    if mark + 1800 < dt2ts(datetime.utcnow()):
                        log('-- MARK --')
                        mark = dt2ts(datetime.utcnow())


    def process_objects(self, objs, kind, booting=False):
        pass


    def added_object(self, o):
        pass


    def modified_object(self, old, new):
        pass


    def deleted_object(self, o):
        pass


    def delete_object(self, o, delete_func):
        self.dq.put({'object': o, 'func': delete_func})


    def _update_worker(self, list_func, *vargs, **kwargs):
        log('Started stream thread {} with {}'.format(get_ident(), list_func.__name__))

        while True:
            try:
                log('Loading objects with %s...' % list_func.__name__)

                obj_list = list_func(*vargs, **kwargs)
                kind, api_version = obj_list.kind[:-4], obj_list.api_version
                self._enqueue_event({
                    'type': 'FLUSH',
                    'object': kind
                })
                self._enqueue_event({
                    'type': 'UPDATE_RESOURCEVERSION',
                    'object': obj_list.metadata.resource_version
                })

                for o in obj_list.items:
                    o.kind = kind
                    o.api_version  = api_version

                    self._enqueue_event({
                        'type': 'INIT',
                        'object': o
                    })

                self._enqueue_event({
                    'type': 'BOOT',
                    'object': None
                })

                for ev in self.w.stream(list_func, resource_version=obj_list.metadata.resource_version, *vargs, **kwargs):
                    self._enqueue_event(ev)

                log('Stream %s was closed. Restarting...' % list_func.__name__)
            except Exception as ex:
                traceback.print_exc()
                log('Stream thread %s has failed: %s:%s. Restarting...' % (list_func.__name__, ex.__class__, ex))

            time.sleep(1)


    def _enqueue_event(self, ev):
        self.q.put(ev)


    def _process_event(self, ev):
        t, o = ev['type'], ev['object']

        if t == 'INIT':
            self._cache_add(o)
        elif t == 'BOOT':
            self._process_objects(booting=True)
        elif t == 'FLUSH':
            self._cache_flush(o)
        elif t == 'UPDATE_RESOURCEVERSION':
            self._update_rv(o)
        elif t == 'ADDED':
            self._cache_add(o)
            self.added_object(o)
        elif t == 'MODIFIED':
            old, new = self._cache_get(o), o
            self.modified_object(old, new)
            self._cache_update(new)
        elif t == 'DELETED':
            self.deleted_object(o)
            self._cache_delete(o)
        else:
            log('Unhandled event:', t)


    def _update_rv(self, rv):
        try:
            rv = int(rv)
            if self.rv < rv:
                self.rv = rv
        except TypeError:
            pass

    def _cache_flush(self, kind):
        with self.l:
            if kind in self.c:
                del self.c[kind]
                self.c[kind] = {}


    def _cache_add(self, o):
        with self.l:
            #log('+', oid(o))
            if o.kind not in self.c:
                self.c[o.kind] = {}
            self.c[o.kind][o.metadata.uid] = o


    def _cache_get(self, o):
        with self.l:
            if o.kind in self.c:
                return self.c[o.kind].get(o.metadata.uid, None)


    def _cache_update(self, o):
        #log('=', oid(o))
        uid = o.metadata.uid
        with self.l:
            if o.kind not in self.c:
                self.c[o.kind] = {}
            self.c[o.kind][uid] = o


    def _cache_delete(self, o):
        uid = o.metadata.uid
        with self.l:
            if o.kind in self.c:
                if uid in self.c[o.kind]:
                    #log('-', oid(o))
                    del self.c[o.kind][uid]


    def _process_objects(self, booting):
        with self.l:
            for kind, objs in self.c.items():
                self.process_objects(objs, kind, booting)


    def _delete_worker(self):
        log('Started delete thread {}'.format(get_ident()))
        while True:
            try:
                while True:
                    d = self.dq.get(block=True)
                    o, df = d['object'], d['func']
                    df(o)
                    self.dq.task_done()
            except:
                traceback.print_exc()
                log('Delete thread has died. Restarting...')

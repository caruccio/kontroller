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
        log('Loading objects with %s...' % list_func.__name__)
        self._cache_add_list(list_func(*vargs, **kwargs))
        self._process_objects(booting=True)

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


    def process_objects(self, *os):
        pass


    def added_object(self, o):
        pass


    def modified_object(self, old, new):
        pass


    def deleted_object(self, o):
        pass


    def delete_object(self, o, delete_func):
        self.dq.put({'object': o, 'func': delete_func})


    def _cache_add_list(self,  obj_list):
        kind, api_version = obj_list.kind[:-4], obj_list.api_version
        self._update_rv(obj_list.metadata.resource_version)
        for o in obj_list.items:
            o.kind = kind
            o.api_version  = api_version
            self._cache_add(o)
            self._update_rv(o.metadata.resource_version)


    def _update_rv(self, rv):
        rv = int(rv)
        if self.rv < rv:
            self.rv = rv


    def _update_worker(self, list_func, *vargs, **kwargs):
        log('Started stream thread {} with {}'.format(get_ident(), list_func.__name__))
        while True:
            try:
                for ev in self.w.stream(list_func, resource_version=self.rv, *vargs, **kwargs):
                    self._enqueue_event(ev)
            except:
                traceback.print_exc()
                log('Stream thread has died. Restarting...')
                time.sleep(3)


    def _enqueue_event(self, ev):
        self.q.put(ev)


    def _process_event(self, ev):
        t, o = ev['type'], ev['object']

        if t == 'ADDED':
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
            log('Unhandled Event:', t, ev['raw_object'])
            return

        self._update_rv(o.metadata.resource_version)


    def _cache_add(self, o):
        with self.l:
            #log('+', oid(o))
            self.c[o.metadata.uid] = o


    def _cache_get(self, o):
        with self.l:
            return self.c.get(o.metadata.uid, None)


    def _cache_update(self, o):
        #log('=', oid(o))
        uid = o.metadata.uid
        with self.l:
            self.c[uid] = o


    def _cache_delete(self, o):
        uid = o.metadata.uid
        with self.l:
            if uid in self.c:
                #log('-', oid(o))
                del self.c[uid]


    def _process_objects(self, booting):
        with self.l:
            return bool(self.process_objects(self.c, booting))


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

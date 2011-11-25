#!/usr/bin/env python

""" """

from itertools import ifilter

from PySide.QtCore import QObject, QSocketNotifier, QCoreApplication, QTimer

from pika.utils import cached
from pika.adapters.base_connection import BaseConnection
from pika.adapters.base_connection import READ, WRITE, ERROR


class PySideConnectionPoller(QObject):

    def __init__(self, connection):
        # Set container
        self.parent = connection

    def __iter__(self):
        return iter((self.reader, self.writer,))

    def _connect(self, notifier_type, callback):
        notifier = QSocketNotifier(self.parent.fileno, notifier_type)
        notifier.activated.connect(callback)
        notifier.setEnabled(False)
        return notifier

    def _read(self, _):
        self.parent._handle_read()
        self.parent._manage_event_state()

    def _write(self, _):
        self.parent._handle_write()
        self.parent._manage_event_state()

    def _error(self, _):
        self.parent._handle_disconnect()

    def poll(self):
        # Create Notifiers
        self.reader = self._connect(QSocketNotifier.Read,  self._read)
        self.writer = self._connect(QSocketNotifier.Write, self._write)
        # Create Error watcher
        self.errors = self._connect(QSocketNotifier.Exception, self._error)
        self.errors.setEnabled(True)
        # update handlers
        self.parent.ioloop.update_handler(None, self.parent.event_state)

    def unpoll(self):
        self.reader = self.writer = self.errors = None


class PySideConnection(BaseConnection):

    def __iter__(self):
        return iter(self.notifiers)

    def _adapter_connect(self):
        # Connect (blockignly!) to the server
        BaseConnection._adapter_connect(self)
        self.event_state |= WRITE
        # Setup the IOLoop
        self.ioloop = IOLoop(self.notifiers)
        # Let everyone know we're connected
        self._on_connected()

    def _flush_outbound(self):
        self._manage_event_state()

    @property
    def fileno(self):
        return self.socket.fileno()

    @property
    @cached
    def notifiers(self):
        return PySideConnectionPoller(self)


class IOLoop(QObject):
    def __init__(self, poller):
        self.poller = poller

    def stop(self):
        QTimer.singleShot(0, self.poller.unpoll)
        self.exec_ and QCoreApplication.instance().quit()

    def start(self, exec_=True):
        self.exec_ = exec_
        QTimer.singleShot(0, self.poller.poll)
        self.exec_ and QCoreApplication.instance().exec_()

    def remove_handler(self, fdn=None):
        [notifier.setEnabled(False) for notifier in self.poller]

    def update_handler(self, fdn, event_state):
        self.remove_handler()
        # update notifiers state
        if event_state & READ:
            self.poller.reader.setEnabled(True)
        if event_state & WRITE:
            self.poller.writer.setEnabled(True)

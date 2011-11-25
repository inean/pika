
from pika.adapters.base_connection import BaseConnection
from pika.adapters.base_connection import READ, WRITE, ERROR

class PySideConnectionPoller(QObject):

    def __init__(self, connection):
        # Set container
        self.parent = connection
        # Create Notifiers
        self.reader = self._connect(QSocketNotifier.Read,  self._read)
        self.writer = self._connect(QSocketNotifier.Write, self._write)
        self.errors = self._connect(QSocketNotifier.Error, self._error)

    def _connect(self, notifier_type, callback):
        notifier = QSocketNotifier(self.parent.fileno, notifier_type)
        notifier.activated.connect(callback)
        return notifier
    
    def _read(self, _):
        self.parent._handle_read()
            
    def _write(self, _):
        self.parent._handle_write()

    def _error(self, _):
        self.parent._handle_error()


class PySideConnection(BaseConnection):

    def __iter__(self):
        notifier = self.notifier
        return (notifier.reader, notifier.writer, notifier.errors)

    def _adapter_connect(self):
        # Connect (blockignly!) to the server
        BaseConnection._adapter_connect(self)
        # Setup the IOLoop
        self.ioloop = IOLoop(self)
        # Set the I/O events we're waiting for (see IOLoopReactorAdapter
        self.ioloop.update_handler(self.fileno, self.event_state)
        # Let everyone know we're connected
        self._on_connected()

    def _adapter_disconnect(self):
        # Remove from the IOLoop
        self.ioloop.remove_handler(None)
        BaseConnection._adapter_disconnect(self)

    def _on_connected(self):
        BaseConnection._on_connected(self)
        # flush connected frame
        self._flush_outbound()

    @property
    def fileno(self):
        return self.socket.fileno()

    @property
    @cached
    def notifiers(self):
        return PySideConnectionPoller(self)


class IOLoop(QObject):
    def __init__(self, connnection):
        self.connection = connection

    def stop(self):
        QCoreApplication.instance().quit()

    def start(self):
        application = QCoreApplication.instance()
        application and application.exec_()

    def remove_handler(self, fdn = None):
        # disable all notifiers
        for notifier in self.connection:
            notifier.enable = False

    def update_handler(self, fdn, event_state):
        # disable all notifiers
        self.remove_handler()
        # update notifiers state
        if event_state % READ:
            self.notifiers.read.enable = True
        if event_state % WRITE:
            self.notifiers.read.enable = True
        if event_state % ERROR:
            raise NotImplementedError



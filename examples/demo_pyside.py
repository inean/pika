# ***** BEGIN LICENSE BLOCK *****
#
# For copyright and licensing please refer to COPYING.
#
# ***** END LICENSE BLOCK *****

"""
Example of simple consumer. Acks each message as it arrives.
"""
import sys
import signal

# Detect if we're running in a git repo
from os.path import exists, normpath
if exists(normpath('../pika')):
    sys.path.insert(0, '..')

from pika.adapters.pyside_connection import PySideConnection
from pika.adapters.pyside_connection import PySideReconnectionStrategy

from pika.connection import ConnectionParameters

# import PySide
from PySide.QtCore import QCoreApplication, QTimer

# We use these to hold our connection & channel
connection = None
channel = None


def on_connected(connection):
    global channel
    print "demo_receive: Connected to RabbitMQ"
    connection.channel(on_channel_open)


def on_channel_open(channel_):
    global channel
    channel = channel_
    print "demo_receive: Received our Channel"
    channel.queue_declare(queue="test",
                          durable=True,
                          exclusive=False,
                          auto_delete=False,
                          callback=on_queue_declared)


def on_queue_declared(frame):
    print "demo_receive: Queue Declared"
    channel.basic_consume(handle_delivery, queue='test')


def handle_delivery(channel, method_frame, header_frame, body):
    print "Basic.Deliver %s delivery-tag %i: %s" %\
          (header_frame.content_type,
           method_frame.delivery_tag,
           body)
    channel.basic_ack(delivery_tag=method_frame.delivery_tag)


if __name__ == '__main__':

    def shutdown(*args):
        # Close the connection
        connection.close()
        # Loop until the conneciton is closed
        connection.ioloop.stop()

    # Connect to RabbitMQ
    host = (len(sys.argv) > 1) and sys.argv[1] or '127.0.0.1'
    # create a Qt Application
    application = QCoreApplication(sys.argv)
    # pyside connection. Reconnection strategy requires that an
    # application is defined (Eventloop)
    connection = PySideConnection(
        ConnectionParameters(host),
        on_connected,
        PySideReconnectionStrategy())
    # Loop until CTRL-C
    # shutdown app at 20s
    QTimer.singleShot(20000, shutdown)
    # Add a Control-C interrupt
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    # Start our blocking loop
    print "Press Ctrl-C ow wait 20 seconds for a clean shutdown\n"
    connection.ioloop.start()

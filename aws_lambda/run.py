from distributed import Worker
from distributed.cli import dask_worker
from distributed.worker import LambdaWorker
from distributed.comm import get_local_address_for
from tornado.ioloop import IOLoop
from tornado import gen
import multiprocessing
import threading
import paramiko
import socket
import select
import boto3
import time
import os

SSH_PORT = 22
s3 = boto3.resource('s3')


def handler(sock, chan, host, port):
    print('running handler')
    sock = socket.socket()
    try:
        sock.connect((host, port))
    except Exception as e:
        print('Forwarding request to %s:%d failed: %r' % (host, port, e))
        return

    print('Connected!  Tunnel open %r -> %r -> %r' % (chan.origin_addr,
                                                      chan.getpeername(), (host, port)))
    while True:
        r, w, x = select.select([sock, chan], [], [])
        if sock in r:
            data = sock.recv(1024)
            if len(data) == 0:
                break
            chan.send(data)
        if chan in r:
            data = chan.recv(1024)
            if len(data) == 0:
                break
            sock.send(data)
    chan.close()
    sock.close()
    print('Tunnel closed from %r' % (chan.origin_addr,))


def reverse_forward_tunnel(sock, server_port, remote_host, remote_port, transport):
    print('running reverse_forward_tunnel')
    transport.request_port_forward('', server_port)
    print('Starting loop')
    while True:
        print('Running loop')
        chan = transport.accept(1000)
        print('Accepted chan')
        if chan is None:
            continue
        handler(sock, chan, remote_host, remote_port)


def forward(sock, user, ssh_key, remote_url, remote_port, local_port):
    client = paramiko.SSHClient()
    client.load_system_host_keys()
    client.set_missing_host_key_policy(paramiko.client.AutoAddPolicy())
    client.connect(remote_url, 22, username=user, key_filename=ssh_key)

    my_local_ip = get_local_address_for('{}:{}'.format(remote_url, remote_port))[6:]

    #t = threading.Thread(target=reverse_forward_tunnel, args=(sock, remote_port, 'localhost', local_port, client.get_transport()))
    #t.setDaemon(True)
    #t.start()
    #time.sleep(60)
    #reverse_forward_tunnel(sock, remote_port, 'localhost', local_port, client.get_transport())
    reverse_forward_tunnel(sock, remote_port, my_local_ip, local_port, client.get_transport())

    #return t


def run_worker(scheduler_addr):
    print('Starting dask worker')
    loop = IOLoop.current()
    w = LambdaWorker('tcp://' + scheduler_addr, loop=loop, local_dir='/tmp/', contact_address=None, name='', reconnect=False)

    @gen.coroutine
    def run():
        written = False
        yield w._start(None)
        while w.status != 'closed':
            if not written:
                with open('/tmp/port', 'w') as f:
                    f.write(str(w.port))
                written = True
            yield gen.sleep(0.2)


    try:
        loop.run_sync(run)
    except (KeyboardInterrupt, TimeoutError):
        pass
    finally:
        print("End worker")


def run_forward(user, new_cert_location, scheduler_ip):
    while not os.path.exists('/tmp/port'):
        time.sleep(0.2)

    port = int(open('/tmp/port', 'r').read())
    forward(None, user, new_cert_location, scheduler_ip, port, port)


def run(event, context):
    print('Starting')
    if os.path.exists('/tmp/port'):
        os.remove('/tmp/port')

    scheduler_ip = event['ip']
    scheduler_port = event['port']
    #worker_remote_port = event['worker_remote_port']

    user = event['user']
    ssh_key = event['ssh_key']
    new_cert_location = '/tmp/cert.pem'

    print('Creating s3 object')
    obj = s3.Object(*ssh_key.split('/', 1))
    print('Downloading s3 object')
    obj.download_file(new_cert_location)
    print('Changing file permissions on cert')
    os.chmod(new_cert_location, 400)


    scheduler_addr = '{}:{}'.format(scheduler_ip, scheduler_port)
    print('Starting worker connecting to scheduler at: {}'.format(scheduler_addr))
    p_worker = multiprocessing.Process(target=run_worker, args=(scheduler_addr,))
    p_worker.start()

    time.sleep(1)
    p_forwarder = multiprocessing.Process(target=run_forward, args=(user, new_cert_location, scheduler_ip))
    p_forwarder.start()

    time.sleep(12)

    print('Returning')


if __name__ == '__main__':
    event = {
        "ip": "34.245.189.22",
        "port": 8786,
        "user": "ec2-user",
        "ssh_key": "mikeokslonger-dask/ec2.pem"
    }
    run(event, None)
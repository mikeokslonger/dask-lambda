from distributed import Worker
import threading
import paramiko
import socket
import select
import boto3
import os

SSH_PORT = 22
s3 = boto3.resource('s3')


def handler(chan, host, port):
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


def reverse_forward_tunnel(server_port, remote_host, remote_port, transport):
    transport.request_port_forward('', server_port)
    while True:
        chan = transport.accept(1000)
        if chan is None:
            continue
        thr = threading.Thread(target=handler, args=(chan, remote_host, remote_port))
        thr.setDaemon(True)
        thr.start()


def forward(user, ssh_key, remote_url, remote_port, local_port):
    client = paramiko.SSHClient()
    client.load_system_host_keys()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(remote_url, 22, username=user, key_filename=ssh_key)
    t = threading.Thread(target=reverse_forward_tunnel, args=(remote_port, 'localhost', local_port, client.get_transport()))
    t.setDaemon(True)
    t.start()


def run(event, context):
    print('Starting')
    scheduler_ip = event['ip']
    scheduler_port = event['port']
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


    print('Starting dask worker')
    w = Worker('tcp://' + scheduler_addr, local_dir='/tmp/')
    w._start('localhost')

    import time
    time.sleep(5)

    print('Starting forwarding')
    forward(user, new_cert_location, scheduler_ip, remote_port=w.port, local_port=w.port)

    time.sleep(10)

    print('Returning')
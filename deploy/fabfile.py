import os

from fabric import task, Connection
from invoke import UnexpectedExit

PASSWORD = os.environ['KHOURY_PASSWORD']
SSH_KEY_PATH = '~/.ssh/id_rsa'
ETCD_DIR = '~/etcd'
SERVER_DIR = ETCD_DIR + '/server'

HTTP_SCHEME = 'http://'


class EtcdConfig:
    ip: str
    port: int
    name: str

    listenClientUrls: str
    advertiseClientUrls: str

    initialAdvertisePeerUrls: str
    listenPeerUrls: str
    initialCluster: str

    preVote: str = "false"
    logLevel: str = "debug"

    def __str__(self):
        return '--data-dir=' + 'data.etcd.' + self.name \
            + ' --name=' + self.name \
            + ' --listen-client-urls=' + self.listenClientUrls \
            + ' --advertise-client-urls=' + self.advertiseClientUrls \
            + ' --initial-advertise-peer-urls=' + self.initialAdvertisePeerUrls \
            + ' --listen-peer-urls=' + self.listenPeerUrls \
            + ' --initial-cluster=' + self.initialCluster \
            + ' --pre-vote=' + self.preVote \
            + ' --log-level=' + self.logLevel


def parse_configs(cluster_url: str) -> dict:
    if len(cluster_url) == 0:
        print("empty cluster url")
        exit(1)

    configs = dict()
    for node in cluster_url.split(","):
        tokens = node.split("=")
        name = tokens[0]
        url = tokens[1]

        tokens = url.replace(HTTP_SCHEME, "").split(':')
        ip = tokens[0]
        port = int(tokens[1])

        config = EtcdConfig()
        config.ip = ip
        config.port= port
        config.name = name

        # client comm
        config.listenClientUrls = HTTP_SCHEME + ip + ':' + str(port-1)
        config.advertiseClientUrls = HTTP_SCHEME + ip + ':' + str(port-1)

        # peer comm
        config.initialAdvertisePeerUrls = url
        config.listenPeerUrls = url
        config.initialCluster = cluster_url

        configs[ip] = config

    return configs


@task
def start(ctx, cluster_url):
    """
    cluster_url format: 1=http://127.0.0.1:1380,2=http://127.0.0.1:2380,3=http://127.0.0.1:3380,4=http://127.0.0.1:4380,5=http://127.0.0.1:5380,6=http://127.0.0.1:6380
    """
    configs = parse_configs(cluster_url)
    for ip in configs:
        cfg = configs[ip]
        run_cmd(ip, 'cd {} && '.format(SERVER_DIR) +
                'nohup {} > etcd.{}.out 2>&1 &'.format('./server ' + str(cfg), cfg.name))
        print("started " + ip)


@task
def stop(ctx, cluster_url):
    """
    cluster_url format: 1=http://127.0.0.1:1380,2=http://127.0.0.1:2380,3=http://127.0.0.1:3380,4=http://127.0.0.1:4380,5=http://127.0.0.1:5380,6=http://127.0.0.1:6380
    """
    configs = parse_configs(cluster_url)
    for ip in configs:
        cfg = configs[ip]
        run_cmd(cfg.ip, 'kill -9 $(/usr/sbin/lsof -t -i:{})'.format(cfg.port))
        print("stopped " + ip)


@task
def clean(ctx, cluster_url):
    """
    cluster_url format: 1=http://127.0.0.1:1380,2=http://127.0.0.1:2380,3=http://127.0.0.1:3380,4=http://127.0.0.1:4380,5=http://127.0.0.1:5380,6=http://127.0.0.1:6380
    """
    configs = parse_configs(cluster_url)
    for ip in configs:
        cfg = configs[ip]
        run_cmd(cfg.ip, 'cd {} && '.format(SERVER_DIR) +
                'rm -rf data.etcd.* && rm etcd.*.out')
        print("cleaned " + ip)


@task
def logs(ctx, ip):
    """
    cluster_url format: 1=http://127.0.0.1:1380,2=http://127.0.0.1:2380,3=http://127.0.0.1:3380,4=http://127.0.0.1:4380,5=http://127.0.0.1:5380,6=http://127.0.0.1:6380
    """
    os.system('scp -i {} {}:{}/etcd.*.out .'.format(SSH_KEY_PATH, ip, SERVER_DIR))


def run_cmd(ip, cmd):
    with Connection(host=ip, connect_kwargs={'password': PASSWORD}) as conn:
        try:
            conn.run(cmd, asynchronous=True)
        except UnexpectedExit:
            print("run cmd failed: " + cmd)

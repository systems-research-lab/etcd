import sys
import yaml
import shlex
import time
import subprocess
import logging
import fabfile as fab
import copy
import invoke


logstream = open('run.log', 'w', buffering=10)
logging.basicConfig(stream=logstream, format='%(asctime)s - %(levelname)s - %(message)s')
LOGGER = logging.getLogger("batch_run")
LOGGER.setLevel(level=logging.INFO)


def read_config():
    with open('config.yaml', 'r') as file:
        config = yaml.safe_load(file)
    return config


def _split_performance(config):
    assert config['type'] == 'split'

    for c in [2,3]:
        nodes = []
        for clr in config['clusters'][:c]:
            for node in clr:
                nodes.append(node)
        cluster_url = ','.join([f'{idx+1}={node}' for idx, node in enumerate(nodes)])
    
        for t in [128]: # thread
            r = 1
            while r < 6: # repetition
                tmp_config = copy.deepcopy(config)
                tmp_config['clients'] = config['clients'][:c]
                tmp_config['clusters'] = config['clusters'][:c]
                tmp_config['threads'] = t
                tmp_config['repetition'] = r

                LOGGER.info(f'=========================Thread={t} Repetition={r}=========================')
                fab.start(invoke.context.Context(), cluster_url, logging='panic')
                LOGGER.info(f'Running experiment...')
                time.sleep(10)
                if _go(tmp_config, "split_perf.go util.go", 60*5):
                    LOGGER.info('Experiment succeed!')
                    r += 1
                else:
                    LOGGER.error('Experiment failed!')
                fab.clean(invoke.context.Context(), cluster_url)
                LOGGER.info(f'===========================================================================\n')
                time.sleep(15)


def _merge_performance(config):
    assert config['type'] == 'merge'

    for c in [3]:
        cluster_urls = []
        for clr in config['clusters'][:c]:
            nodes = []
            for node in clr:
                nodes.append(node)
            url = ','.join([f'{idx+1}={node}' for idx, node in enumerate(nodes)])
            cluster_urls.append(url)
    
        for t in [16]: # thread
            r = 1
            while r < 6: # repetition
                tmp_config = copy.deepcopy(config)
                tmp_config['clients'] = config['clients'][:c]
                tmp_config['clusters'] = config['clusters'][:c]
                tmp_config['threads'] = t
                tmp_config['repetition'] = r

                LOGGER.info(f'=========================Thread={t} Repetition={r}=========================')
                for url in cluster_urls:
                    LOGGER.info("url: " + url)
                    fab.start(invoke.context.Context(), url, logging='panic')
                LOGGER.info(f'Running experiment...')
                time.sleep(10)
                if _go(tmp_config, "merge_perf.go util.go", 60*5):
                    LOGGER.info('Experiment succeed!')
                    r += 1
                else:
                    LOGGER.error('Experiment failed!')
                    subprocess.run(["killall", "merge_perf"]) 
                    for cli in tmp_config['clients']:
                        subprocess.run(['ssh', f'ubuntu@{cli}', 'killall', '-9', 'merge_perf_client'])
                for url in cluster_urls:
                    fab.clean(invoke.context.Context(), url)
                LOGGER.info(f'===========================================================================\n')
                time.sleep(15)


def _qps_performance(config):
    assert config['type'] == 'qps'

    clients = config['clients'][:8]
    cluster = config['clusters'][:1]

    remoteServerDir = {"sm": "~/etcd/server", "origin": "~/etcd-origin/server"}

    for version in ['sm', 'origin']:
        nodes = []
        for clr in cluster:
            for node in clr:
                nodes.append(node)
        cluster_url = ','.join([f'{idx+1}={node}' for idx, node in enumerate(nodes)])
    
    
        # for t in [1,4,8,16,32,50,64,100,128,150,200,256,300,350,400,450,512]: # thread
        # for t in [1,4,8,16,32,64,128,256,512,1024]: # thread
        # for t in [1,2,4,8,16,32,64,128,256,512]: # thread
        for t in [4096]: # thread
            r = 1
            while r < 4: # repetition
                tmp_config = copy.deepcopy(config)
                tmp_config['clients'] = clients
                tmp_config['clusters'] = cluster
                tmp_config['threads'] = t
                tmp_config['repetition'] = r
                tmp_config['version'] = version

                LOGGER.info(f'=========================Version={version} Thread={t} Repetition={r}=========================')
                fab.start(invoke.context.Context(), cluster_url, logging='panic', remoteServerDir = remoteServerDir[version])
                LOGGER.info(f'Running experiment...')
                time.sleep(10)
                if _go(tmp_config, "qps_perf.go util.go", 60*8):
                    LOGGER.info('Experiment succeed!')
                    r += 1
                else:
                    subprocess.run(["killall", "qps_perf"]) 
                    for cli in tmp_config['clients']:
                        subprocess.run(['ssh', f'ubuntu@{cli}', 'killall', '-9', 'qps_perf_client'])
                    LOGGER.error('Experiment failed!')
                fab.clean(invoke.context.Context(), cluster_url,  remoteServerDir = remoteServerDir[version])
                LOGGER.info(f'=============================================================================================\n')
                time.sleep(15)


def _go(config, run_arg, timeout):
    fn = "generated_config.yaml"
    with open(fn, 'w', encoding='utf-8') as f:
        yaml.dump(config, f)

    try:
        subprocess.run(shlex.split(f'go run {run_arg} --config {fn}'),
                              stdout=logstream, stderr=logstream, timeout=timeout, check=True)
        return True
    except subprocess.CalledProcessError as e:
        LOGGER.error(f'Returned error! errcode={e.returncode}\n{e}')
    except subprocess.TimeoutExpired as e:
        LOGGER.error(f'Process timeout. {e}')
    except Exception as e:
        LOGGER.error(f'Unknown exception. {e}')
    return False


if __name__ == "__main__":
    cfg = read_config()
    type = cfg['type']
    LOGGER.info("Experiment type: " + type)
    if type == "split":
        _split_performance(cfg)
    elif type == "merge":
        _merge_performance(cfg)
    elif type == "qps":
        _qps_performance(cfg)
    else:
        LOGGER.error("unknown type: " + cfg.Type)
        sys.Exit(1)
	
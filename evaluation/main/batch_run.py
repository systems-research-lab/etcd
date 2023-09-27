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
    assert config['type'] == 'split-performance'
    assert config['load'] == 0

    nodes = []
    for clr in config['clusters']:
        for node in clr:
            nodes.append(node)
    cluster_url = ','.join([f'{idx+1}={node}' for idx, node in enumerate(nodes)])
    
    for t in [128,256]: # thread
        r = 1
        while r < 6: # repetition
            config['threads'] = t
            config['repetition'] = r

            LOGGER.info(f'=========================Thread={t} Repetition={r}=========================')
            fab.start(invoke.context.Context(), cluster_url, logging='panic')
            LOGGER.info(f'Running experiment...')
            time.sleep(10)
            if _go(config, 120):
                LOGGER.info('Experiment succeed!')
                r += 1
            else:
                LOGGER.error('Experiment failed!')
            fab.clean(invoke.context.Context(), cluster_url)
            time.sleep(10)
            LOGGER.info(f'===========================================================================\n')
            time.sleep(60)

def _merge_performance(config):
    assert config['type'] == 'merge'
    assert config['load'] == 0

    nodes = []
    cluster_urls = []
    for clr in config['clusters']:
        nodes = []
        for node in clr:
            nodes.append(node)
        cluster_urls.append(','.join([f'{idx+1}={node}' for idx, node in enumerate(nodes)]))
    
    for t in [64,128,256,512]: # thread
        r = 1
        while r < 6: # repetition
            config['threads'] = t
            config['repetition'] = r

            LOGGER.info(f'=========================Thread={t} Repetition={r}=========================')
            for url in cluster_urls:
                fab.start(invoke.context.Context(), url, logging='panic')
            LOGGER.info(f'Running experiment...')
            time.sleep(10)
            if _go(config, 180):
                LOGGER.info('Experiment succeed!')
                r += 1
            else:
                LOGGER.error('Experiment failed!')
            for url in cluster_urls:
                fab.clean(invoke.context.Context(), url)
            time.sleep(10)
            LOGGER.info(f'===========================================================================\n')
            time.sleep(60)


def _split_load(config):
    assert config['type'] == 'split-load'
    assert config['threads'] == 0

    for c in [2]:
        nodes = []
        for clr in config['clusters'][:c]:
            for node in clr:
                nodes.append(node)
        cluster_url = ','.join([f'{idx+1}={node}' for idx, node in enumerate(nodes)])

        for l in [100,1000,10000]: # thread
            r = 1
            while r < 6: # repetition
                tmp_config = copy.deepcopy(config)
                tmp_config['clusters'] = tmp_config['clusters'][:c]
                tmp_config['load'] = l
                tmp_config['repetition'] = r

                LOGGER.info(f'=========================Load={l} Repetition={r}=========================')
                fab.start(invoke.context.Context(), cluster_url, logging='panic')
                LOGGER.info(f'Running experiment...')
                time.sleep(10)
                if _go(tmp_config, 18*60):
                    LOGGER.info('Experiment succeed!')
                    r += 1
                else:
                    LOGGER.error('Experiment failed!')
                fab.clean(invoke.context.Context(), cluster_url)
                time.sleep(10)
                LOGGER.info(f'===========================================================================\n')
                time.sleep(15)


def _merge_load(config):
    assert config['type'] == 'merge'
    assert config['threads'] == 0

    for c in [2,3]:
        cluster_urls = []
        for clr in config['clusters'][:c]:
            nodes = []
            for node in clr:
                nodes.append(node)
            url = ','.join([f'{idx+1}={node}' for idx, node in enumerate(nodes)])
            cluster_urls.append(url)
    
        for l in [100000]: # thread
            r = 1
            while r < 2: # repetition
                tmp_config = copy.deepcopy(config)
                tmp_config['clusters'] = tmp_config['clusters'][:c]
                tmp_config['load'] = l
                tmp_config['repetition'] = r

                LOGGER.info(f'=========================Thread={l} Repetition={r}=========================')
                for url in cluster_urls:
                    fab.start(invoke.context.Context(), url, logging='panic')
                LOGGER.info(f'Running experiment...')
                time.sleep(10)
                if _go(tmp_config, 20*60):
                    LOGGER.info('Experiment succeed!')
                    r += 1
                else:
                    LOGGER.error('Experiment failed!')
                for url in cluster_urls:
                    fab.clean(invoke.context.Context(), url)
                time.sleep(10)
                LOGGER.info(f'===========================================================================\n')
                time.sleep(15)


def _bench_split(config):
    assert config['type'] == 'bench-split'
    assert config['threads'] == 0

    for c in [2]:
        nodes = []
        for clr in config['clusters'][:c]:
            for node in clr:
                nodes.append(node)
        cluster_url = ','.join([f'{idx+1}={node}' for idx, node in enumerate(nodes)])
    
        for l in [1000]: # thread
            r = 1
            while r < 2: # repetition
                tmp_config = copy.deepcopy(config)
                tmp_config['clusters'] = tmp_config['clusters'][:c]
                tmp_config['load'] = l
                tmp_config['repetition'] = r

                LOGGER.info(f'=========================Thread={l} Repetition={r}=========================')
                LOGGER.info(f'Running experiment...')
                time.sleep(10)
                if _go(tmp_config, 10*60):
                    LOGGER.info('Experiment succeed!')
                    r += 1
                else:
                    LOGGER.error('Experiment failed!')
                fab.clean(invoke.context.Context(), cluster_url)
                time.sleep(10)
                LOGGER.info(f'===========================================================================\n')
                time.sleep(15)



def _bench_merge(config):
    assert config['type'] == 'bench-merge'
    assert config['threads'] == 0

    for c in [2,3]:
        nodes = []
        for clr in config['clusters'][:c]:
            for node in clr:
                nodes.append(node)
        cluster_url = ','.join([f'{idx+1}={node}' for idx, node in enumerate(nodes)])
    
        for l in [100, 1000, 10000]: # thread
            r = 1
            while r < 6: # repetition
                tmp_config = copy.deepcopy(config)
                tmp_config['clusters'] = tmp_config['clusters'][:c]
                tmp_config['load'] = l
                tmp_config['repetition'] = r

                LOGGER.info(f'=========================Thread={l} Repetition={r}=========================')
                LOGGER.info(f'Running experiment...')
                if _go(tmp_config, 10*60):
                    LOGGER.info('Experiment succeed!')
                    r += 1
                else:
                    LOGGER.error('Experiment failed!')
                fab.clean(invoke.context.Context(), cluster_url)
                time.sleep(10)
                LOGGER.info(f'===========================================================================\n')
                time.sleep(15)


def _go(config, timeout):
    fn = "generated_config.yaml"
    with open(fn, 'w', encoding='utf-8') as f:
        yaml.dump(config, f)
    
    try:
        subprocess.run(shlex.split('go run . --config=' + fn),
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
    if type == "split-performance":
        _split_performance(cfg)
    elif type == "split-load":
        _split_load(cfg)
    elif type == "merge":
        if (cfg['load'] == 0):
            _merge_performance(cfg)
        else:
            _merge_load(cfg)
    elif type == "bench-split":
        _bench_split(cfg)
    elif type == "bench-merge":
        _bench_merge(cfg)
    else:
        LOGGER.error("unknown type: " + cfg.Type)
        sys.Exit(1)
	
#!/bin/bash
sudo apt update
sudo snap install go --classic
sudo apt install pip
pip install fabric
sudo apt install fabric
#env.sh


#!/bin/bash
git clone https://github.com/systems-research-lab/etcd.git
cd ~/etcd
git checkout rcAdd_1
cd ~/etcd && go build .
cd ~/etcd/server && go build .
cd ~/etcd/etcdctl && go build .
#clone_repo.sh

#!/bin/bash
cd etcd
git pull
cd ~/etcd && go build .
cd ~/etcd/server && go build .
cd ~/etcd/etcdctl && go build
#pull_repo.sh

-----BEGIN OPENSSH PRIVATE KEY-----
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
-----END OPENSSH PRIVATE KEY-----

#!/bin/bash
ssh ubuntu@192.168.0.67 "./clone_repo.sh"
yes
ssh ubuntu@192.168.0.67 "./clone_repo.sh"
yes
ssh ubuntu@192.168.0.67 "./clone_repo.sh"
yes
#remote_clone_repo.sh

ssh -o StrictHostKeyChecking=no ubuntu@192.168.0.8 "./clone_repo.sh" & ssh ubuntu@192.168.0.138 "./clone_repo.sh" & ssh ubuntu@192.168.0.12 "./clone_repo.sh" & ssh ubuntu@192.168.0.98 "./clone_repo.sh"

ssh -o StrictHostKeyChecking=no ubuntu@192.168.0.162 "./pull_repo.sh"
ssh -o StrictHostKeyChecking=no ubuntu@192.168.0.8 "./pull_repo.sh" & ssh -o StrictHostKeyChecking=no ubuntu@192.168.0.32 "./pull_repo.sh" & ssh -o StrictHostKeyChecking=no ubuntu@192.168.0.167 "./pull_repo.sh"& ssh -o StrictHostKeyChecking=no ubuntu@192.168.0.176 "./pull_repo.sh"

#!/bin/bash
cd ~/etcd
git checkout $1
cd ~/etcd && go build .
cd ~/etcd/server && go build .
cd ~/etcd/etcdctl && go build .
#checkout_repo.sh
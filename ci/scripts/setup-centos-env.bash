#!/bin/bash

set -ex

ccp_src/scripts/setup_ssh_to_cluster.sh
USER=${USER:=centos}
GPHOME=/usr/local/greenplum-db-devel

ssh -t ${USER}@mdw " \
    GO_VERSION=1.17.6
    sudo yum install -y git && \
    wget https://storage.googleapis.com/golang/go\${GO_VERSION}.linux-amd64.tar.gz && \
    sudo rm -rf /usr/local/go && sudo tar -xzf go\${GO_VERSION}.linux-amd64.tar.gz -C /usr/local && \
    sudo mkdir -p /home/gpadmin/go/src/github.com/greenplum-db && \
    sudo chown gpadmin:gpadmin -R /home/gpadmin"

scp -r -q gpbackup_src mdw:/home/gpadmin/go/src/github.com/greenplum-db/gpbackup

if test -f dummy_seclabel/dummy_seclabel*.so; then
  scp dummy_seclabel/dummy_seclabel*.so mdw:${GPHOME}/lib/postgresql/dummy_seclabel.so
  scp dummy_seclabel/dummy_seclabel*.so sdw1:${GPHOME}/lib/postgresql/dummy_seclabel.so
  scp dummy_seclabel/dummy_seclabel*.so sdw2:${GPHOME}/lib/postgresql/dummy_seclabel.so
  scp dummy_seclabel/dummy_seclabel*.so sdw3:${GPHOME}/lib/postgresql/dummy_seclabel.so
  scp dummy_seclabel/dummy_seclabel*.so sdw4:${GPHOME}/lib/postgresql/dummy_seclabel.so
fi

cat <<SCRIPT > /tmp/setup_env.bash
#!/bin/bash

set -ex
    cat << ENV_SCRIPT > env.sh
    export GOPATH=/home/gpadmin/go
    source ${GPHOME}/greenplum_path.sh
    export PGPORT=5432
    export MASTER_DATA_DIRECTORY=/data/gpdata/master/gpseg-1
    export PATH=\\\${GOPATH}/bin:/usr/local/go/bin:\\\${PATH}
    if [[ -f /opt/gcc_env.sh ]]; then
        source /opt/gcc_env.sh
    fi
ENV_SCRIPT

export GOPATH=/home/gpadmin/go
chown gpadmin:gpadmin -R \${GOPATH}
chmod +x env.sh
source env.sh
gpconfig --skipvalidation -c fsync -v off
if test -f ${GPHOME}/lib/postgresql/dummy_seclabel.so; then
    gpconfig -c shared_preload_libraries -v dummy_seclabel
fi
gpstop -ar
SCRIPT

chmod +x /tmp/setup_env.bash
scp /tmp/setup_env.bash mdw:/home/gpadmin/setup_env.bash
ssh -t mdw "/home/gpadmin/setup_env.bash"

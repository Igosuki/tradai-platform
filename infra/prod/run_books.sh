#!/bin/sh

BASEDIR=$(dirname "$0")

export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES

ansible-playbook -i $BASEDIR/inventory.yml $BASEDIR/playbooks/monitoring.yml
ansible-playbook -i $BASEDIR/inventory.yml $BASEDIR/playbooks/trader.yml -e "INVENTORY_HOST=trader FEEDER_CONFIG_FILE=feeder_prod.yaml FEEDER_REMOTE_DIRECTORY=data FEEDER_DATA_EXPIRY_AGE=56d"
ansible-playbook -i $BASEDIR/inventory.yml $BASEDIR/playbooks/trader.yml -e "INVENTORY_HOST=feeder24 FEEDER_CONFIG_FILE=feeder24_prod.yaml FEEDER_REMOTE_DIRECTORY=data24 FEEDER_DATA_MOVE_MIN_AGE=11h FEEDER_DATA_EXPIRY_AGE=7d"

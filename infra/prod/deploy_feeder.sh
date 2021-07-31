#!/bin/sh

BASEDIR=$(dirname "$0")

export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES

ansible-playbook -i $BASEDIR/inventory.yml $BASEDIR/playbooks/feeder_deploy.yml -e "INVENTORY_HOST=trader FEEDER_CONFIG_FILE=feeder_prod.yaml"

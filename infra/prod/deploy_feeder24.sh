#!/bin/sh

BASEDIR=$(dirname "$0")

export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES

ansible-playbook -i $BASEDIR/inventory.yml $BASEDIR/playbooks/feeder_deploy.yml -e "INVENTORY_HOST=feeder24 FEEDER_CONFIG_FILE=feeder24_prod.yaml"

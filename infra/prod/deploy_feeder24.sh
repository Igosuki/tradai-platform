#!/bin/sh

export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES

ansible-playbook -i inventory.yml playbooks/feeder_deploy.yml -e "INVENTORY_HOST=feeder24 FEEDER_CONFIG_FILE=feeder24_prod.yaml"

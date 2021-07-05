#!/bin/sh

export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES

ansible-playbook -i inventory.yml playbooks/monitoring.yml
ansible-playbook -i inventory.yml playbooks/trader.yml -e "INVENTORY_HOST=trader FEEDER_CONFIG_FILE=feeder_prod.yaml FEEDER_REMOTE_DIRECTORY=data"
ansible-playbook -i inventory.yml playbooks/trader.yml -e "INVENTORY_HOST=feeder24 FEEDER_CONFIG_FILE=feeder24_prod.yaml FEEDER_REMOTE_DIRECTORY=data24 FEEDER_DATA_MOVE_MIN_AGE=11h"

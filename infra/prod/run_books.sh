#!/bin/sh

export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES

ansible-playbook -i inventory.yml playbooks/monitoring.yml
ansible-playbook -i inventory.yml playbooks/trader.yml -e "INVENTORY_HOST=trader"

#!/bin/sh

export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES

ansible-playbook -i inventory.yml playbooks/trader_feeder.yml

#!/bin/sh

export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES

ansible-playbook -i inventory.yml playbooks/feeder_deploy.yml

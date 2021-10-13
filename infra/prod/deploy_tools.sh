#!/bin/sh

BASEDIR=$(dirname "$0")

export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES

ansible-playbook -i $BASEDIR/inventory.yml $BASEDIR/playbooks/tools_deploy.yml

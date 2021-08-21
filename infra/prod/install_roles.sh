#!/bin/sh

BASEDIR=$(dirname "$0")

ansible-galaxy install cloudalchemy.prometheus --force -p $BASEDIR/playbooks/roles
ansible-galaxy install cloudalchemy.node_exporter --force -p $BASEDIR/playbooks/roles
ansible-galaxy install cloudalchemy.pushgateway --force -p $BASEDIR/playbooks/roles
ansible-galaxy install cloudalchemy.grafana --force -p $BASEDIR/playbooks/roles
ansible-galaxy install cloudalchemy.alertmanager --force -p $BASEDIR/playbooks/roles

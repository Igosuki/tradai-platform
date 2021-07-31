#!/bin/sh

BASEDIR=$(dirname "$0")

ansible-galaxy install cloudalchemy.prometheus -p $BASEDIR/playbooks/roles
ansible-galaxy install cloudalchemy.node-exporter -p $BASEDIR/playbooks/roles
ansible-galaxy install cloudalchemy.pushgateway -p $BASEDIR/playbooks/roles
ansible-galaxy install cloudalchemy.grafana -p $BASEDIR/playbooks/roles
ansible-galaxy install cloudalchemy.alertmanager -p $BASEDIR/playbooks/roles

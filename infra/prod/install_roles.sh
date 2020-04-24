#!/bin/sh

ansible-galaxy install cloudalchemy.prometheus -p ./playbooks/roles
ansible-galaxy install cloudalchemy.node-exporter -p ./playbooks/roles
ansible-galaxy install cloudalchemy.pushgateway -p ./playbooks/roles
ansible-galaxy install cloudalchemy.grafana -p ./playbooks/roles
ansible-galaxy install cloudalchemy.alertmanager -p ./playbooks/roles

# Create a new Web Droplet in the nyc2 region
provider "digitalocean" {
  token = var.do_token
}
data "digitalocean_project" "bitcoins" {}

resource "digitalocean_droplet" "trader" {
  image  = "ubuntu-18-04-x64"
  name   = "trader"
  region = "sgp1"
  size   = "s-1vcpu-1gb"
  backups = true
  ipv6 = true
  ssh_keys = [26568141]
  tags = ["trader", "prod"]
}

resource "digitalocean_droplet" "monitoring" {
  image  = "ubuntu-18-04-x64"
  name   = "monitoring"
  region = "sgp1"
  size   = "s-1vcpu-1gb"
  backups = true
  ipv6 = true
  ssh_keys = [26568141]
  tags = ["monitoring", "prod"]
}

resource "digitalocean_firewall" "monitoring" {
  name = "prometheus"

  droplet_ids = [digitalocean_droplet.monitoring.id]

  inbound_rule {
    protocol         = "tcp"
    port_range       = "22"
    source_addresses = ["::/0"]
  }

  inbound_rule {
    protocol         = "tcp"
    port_range       = "9090"
    source_addresses = ["::/0"]
  }

  outbound_rule {
    protocol              = "icmp"
    destination_addresses = ["0.0.0.0/0", "::/0"]
  }
}

resource "digitalocean_firewall" "trader" {
  name = "trader-monitoring"

  droplet_ids = [digitalocean_droplet.monitoring.id]

  inbound_rule {
    protocol         = "tcp"
    port_range       = "22"
    source_addresses = ["::/0"]
  }

  inbound_rule {
    protocol         = "tcp"
    port_range       = "9091"
    source_addresses = [digitalocean_droplet.monitoring.ipv4_address]
  }

  inbound_rule {
    protocol         = "tcp"
    port_range       = "9100"
    source_addresses = [digitalocean_droplet.monitoring.ipv4_address]
  }

  outbound_rule {
    protocol              = "icmp"
    destination_addresses = ["0.0.0.0/0", "::/0"]
  }
}

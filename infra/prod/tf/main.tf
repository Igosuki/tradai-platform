# Create a new Web Droplet in the nyc2 region
provider "digitalocean" {
  token = var.do_token
}
data "digitalocean_project" "bitcoins" {
}
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



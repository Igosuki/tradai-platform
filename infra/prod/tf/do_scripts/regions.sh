curl -s -X GET -H "Content-Type: application/json" \
  -H "Authorization: Bearer ${DIGITALOCEAN_ACCESS_TOKEN}" \
  "https://api.digitalocean.com/v2/regions?page=1&per_page=100" \
  | jq '.regions[].slug' | sort
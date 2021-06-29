curl -s -X GET -H "Content-Type: application/json" \
  -H "Authorization: Bearer ${DIGITALOCEAN_ACCESS_TOKEN}" \
  "https://api.digitalocean.com/v2/sizes?page=1&per_page=100" \
  | jq '.sizes[].slug' | sort
#!/usr/bin/env bash

for n in $(seq 2 72); do
    sudo -u iog psql -U iog cexplorer <<EOF
    select epoch_no, sum(amount) as sum_amount, ph.hash_raw as pool_hash
      from epoch_stake es
      join pool_hash ph on es.pool_id = ph.id
     where epoch_no = $n
     group by epoch_no, pool_hash
     order by sum_amount desc
EOF
done

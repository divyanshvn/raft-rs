#!/usr/bin/env bash

prefix="new_run"

i=0

while [[ -e "${prefix}${i}.txt" ]]; do
	((i++))
done

echo "Writing output to ${prefix}${i}.txt"

cargo test test_re_election -- --no-capture >"${prefix}${i}.txt"

echo "Wrote output to ${prefix}${i}.txt"

#!/bin/bash
COMMAND=$(cat | jq -r '.tool_input.command')
BLOCKED="data/|\.git/|__pycache__|\.env|logs/|node_modules|dist/|build/"

if echo "$COMMAND" | grep -qE "$BLOCKED"; then
 echo "ERROR: Blocked directory pattern" >&2
 exit 2
fi

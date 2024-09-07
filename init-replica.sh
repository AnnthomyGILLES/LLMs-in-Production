#!/bin/bash
set -e

mongosh --eval "
config = {
    '_id': 'rs0',
    'members': [
        { '_id': 0, 'host': 'localhost:27017' }
    ]
};
rs.initiate(config);
"

echo "Waiting for replica set to be initialized..."
until mongosh --eval "rs.status().ok" | grep -q 1; do
    sleep 1
done
echo "Replica set initialized successfully."

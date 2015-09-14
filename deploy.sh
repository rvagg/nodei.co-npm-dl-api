#!/bin/sh

host=nodei.co

rsync --recursive --times --perms --delete --progress --exclude all.json --exclude all.json --exclude all.json --exclude all.json --exclude all.json --exclude all.json --exclude all.json --exclude all.json --exclude all.json --exclude counts.db/ --exclude node_modules --exclude log ./ nodeico@${host}:/home/nodeico/npm-dl-api/
ssh nodeico@${host} 'mkdir -p /home/nodeico/npm-dl-api/log/ && cd /home/nodeico/npm-dl-api &&  npm install'
ssh root@${host} 'service npm-dl-api restart'

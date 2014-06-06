#!/bin/bash
cd /srv/node/bbc-listings
node ./checkForUpdates.js $1 >> ~/log/checkForListings.log 2>&1


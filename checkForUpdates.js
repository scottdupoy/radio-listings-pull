var listings = require('./listings');

if (process.argv.length < 3) {
  console.log('ERROR: db connection string must be supplied as a command line argument');
  return;
}

listings.checkForUpdates(callback, process.argv[2]);

function callback(err) {
    if (err) {
        console.log('RETRIEVAL FAILED: ' + err);
    }
    else {
        console.log('RETRIEVAL SUCCEEDED');
    }
}

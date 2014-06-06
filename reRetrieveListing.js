var listings = require('./listings');

console.log(process.argv.length);

if (process.argv.length < 4) {
  console.log('Please provide a connection string and a programme id argument');
  return;
}

var id = 'http://www.bbc.co.uk/programmes/' + process.argv[3];
console.log('Retrieve: ' + id);

listings.retrieve(process.argv[2], id, { forceReload: true, addAsChain: false }, callback);

function callback(err) {
    if (err) {
        console.log('RETRIEVAL FAILED: ' + err);
    }
    else {
        console.log('RETRIEVAL SUCCEEDED');
    }
}


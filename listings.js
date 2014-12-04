var http = require('http');
var url = require('url');
var db = require('./db');

update = function(callback) {
    var retrieveNewListings = function() {
        db.retrieveAvailableChainListings(function(err, programmeSources) {
            if (err) {
                callback(err);
                return;
            }
            
            var retrieveFirstSource = function(err, sources) {                        
                if (err) {
                    callback(err);
                    return;
                }
            
                if (sources.length == 0) {
                    console.log('INFO: no more programme sources to try and retrieve');
                    callback();
                    return;
                }
                        
                var source = sources[0];
                sources = sources.slice(1, sources.length);
                
                var parsedUrl = url.parse(source);
                retrieveBbc(parsedUrl.host, parsedUrl.pathname, { forceReload: false, addAsChain: false }, function(retrievalErr) {
                    retrieveFirstSource(retrievalErr, sources);
                });
            };
            
            retrieveFirstSource(null, programmeSources);
        });
    }
    
    var checkFirstChain = function(err, chains) {
        if (err) {
            callback(err);
            return;
        }
        
        if (chains.length == 0) {
            retrieveNewListings(err)
            return;
        }
    
        var chain = chains[0];
        chains = chains.slice(1, chains.length);
                
        var parsedUrl = url.parse(chain.source);

        console.log('INFO: checking chain: ' + chain.source);
        retrieveJson(parsedUrl.host, parsedUrl.pathname, function(data, err) {
            if (err) {
                callback(err);
                return;
            }

            console.log('INFO: got data for: ' + chain.source);

            var retrievedChain = {
                source: 'http://' + parsedUrl.host + parsedUrl.pathname,
                timestamp: data.programme.first_broadcast_date
            };
            
            if (data.programme.peers.previous) {
                retrievedChain.previous = 'http://open.live.bbc.co.uk/aps/programmes/' + data.programme.peers.previous.pid + '.json';
                console.log('INFO: chain has previous: ' + retrievedChain.previous);
            }
            if (data.programme.peers.next) {
                retrievedChain.next = 'http://open.live.bbc.co.uk/aps/programmes/' + data.programme.peers.next.pid + '.json';
                console.log('INFO: chain has next:     ' + retrievedChain.next);
            }
            
            if (retrievedChain.next) {
                chains.push({ source: retrievedChain.next, reretrieval: 0 });
            }
            
            if (chain.reretrieval) {
                db.updateChain(retrievedChain, function(err) {
                    checkFirstChain(err, chains);
                });
            }
            else {
                db.addChain(retrievedChain, function(err) {
                    checkFirstChain(err, chains);
                });
            }
        });
    }
    
    db.retrieveChainsToRetrieve(checkFirstChain);
}

exports.checkForUpdates = function(callback, connectionString) {
    // wrap the update call with db connect/disconnect functionality
    db.connect(connectionString, function(connected, err) {
        if (!connected) {
            callback('Could not connect to the db: ' + err);
            return;
        }
        update(function(err) {
            db.disconnect();
            callback(err);
        });
    });
}

exports.retrieve = function(connectionString, urlString, callback) {
    // set default options
    return retrieve(connectionString, urlString, { forceReload: false, addAsChain: false }, callback);
}

exports.retrieve = function(connectionString, urlString, options, callback) {
    db.connect(connectionString, function(connected, err) {
        if (!connected) {
            callback('Could not connect to the db: ' + err);
            return;
        }
       
        var callbackWithDisconnect = function(err) {
            db.disconnect();
            callback(err);
        };
        
        var parsedUrl = url.parse(urlString);
        var host = parsedUrl["host"];
        var pathname = parsedUrl["pathname"];
        if (host.match(/mixcloud/)) {
            manualAddMixCloud(pathname, options, callbackWithDisconnect);
        }
        else if (host.match(/bbc/)) {
            manualAddBbc(pathname, options, callbackWithDisconnect);
        }
        else {
            callbackWithDisconnect('Unrecognised host: ' + host);
        }        
    });
};

function manualAddMixCloud(pathname, options, callback) {
    retrieveJson('api.mixcloud.com', pathname, function(data) {
        callback();
    });
}

function manualAddBbc(pathname, options, callback) {
    // manually entered path so sanitise at this point
    
    // remove slashes
    if (pathname.match(/\/$/)) {
        // strip off trailing slash
        pathname = pathname.substring(0, pathname.length - 1);
    }
    if (pathname.match(/^\//)) {
        // strip off leading slash
        pathname = pathname.substring(1, pathname.length);
    }
    
    // add api prefix and declare the host
    pathname = '/aps/' + pathname + '.json';
    var host = 'open.live.bbc.co.uk';
    
    // adding manually, apply update all if appropriate
    if (options.addAsChain) {
        retrieveBbc(host, pathname, options, function(err) {
            if (err) {
                callback(err);
                return;
            }
            update(callback);
        });
    }
    else {
        retrieveBbc(host, pathname, options, callback);
    }
}

function retrieveBbc(host, pathname, options, callback) {
    
    var source = 'http://' + host + pathname;
    console.log('Retrieving: ' + source);
    
    var addProgramme = function(err, programme) {    
        if (err) {
            callback(err);
            return;
        }
        addProgrammeToDb(programme, callback);
    };
        
    var checkForData = function(err, programme) {
        if (err) {
            callback(err);
            return;
        }
        
        if (!programme || !programme.tracks || programme.tracks.length == 0) {
            console.log('INFO: no tracks for programme: ' + source);
            callback();
            return;
        }
        
        // check if the programme exists
        db.retrieveProgrammeId(source, function(err, programmeId) {
            if (err) { 
                return callback(err);
            }
            if (programmeId && !options.forceReload) {
                console.log('INFO: programme already exists (' + programmeId + ') and not reloading');
                callback();
                return;
            }
            if (programmeId) {
                db.deleteProgramme(programmeId, function(err) {
                    addProgramme(err, programme);
                });
                return;
            }
            addProgramme(null, programme);
        });
    };
        
    var addChain = function(err, data) {
        if (err) {
            callback(err);
            return;
        }
        db.retrieveChain(source, function(err, chain) {
            if (err) {
                callback(err);
                return;
            }
            if (chain) {
                if (/*chain.timestamp != data.timestamp || */chain.previous != data.previous || chain.next != data.next) {
                    db.updateChain(data, function(err) {
                        checkForData(err, data);
                    });
                }
                else {
                    // nothing to do, chain already exists and requires no changes
                    checkForData(null, data);
                }
            }
            else {
                db.addChain(data, function(err) {
                    checkForData(err, data);
                });
            }
        });
    };
    
    // finally, actually get the data and work up the chain of callbacks
    var handleListing = function(err, data) {
        if (err) {
            callback(err);
            return;
        }
        data.source = source;
        if (options.addAsChain) {        
            addChain(null, data);
            return;
        }
        checkForData(null, data);
    };
    
    retrieveBbcListing(host, pathname, options, handleListing);
}

function addProgrammeToDb(programme, callback) {
    var addFirstTrack = function(programmeId, tracks) {
    
        var track = tracks[0];
        tracks = tracks.slice(1, tracks.length);
        
        // sanitise unknown tracks
        console.log('track: ' + track);
        console.log('track.title:  ' + track.title);
        console.log('track.artist: ' + track.artist);
        if (!track.title || track.title.toString().toLowerCase() == "id" || track.title.toString().toLowerCase() == "unknown" || track.toString().title == "") {
            track.title = 'Unknown'
        }
        if (!track.artist || track.artist.toString().toLowerCase() == "id" || track.artist.toString().toLowerCase() == "unknown" || track.artist.toString() == "") {
            track.artist = 'Unknown'
        }
        
        var addNextTrack = function(err) {
            if (err) {
                callback('Problem adding track link: ' + err);
                return;
            }
            // move on to the next track or exit
            if (tracks.length > 0) {
                addFirstTrack(programmeId, tracks);
            }            
            else {
                callback();
            }
        };
                
        var addTrackLink = function(err, trackId) {
            if (err) {
                callback('Problem adding track: ' + err);
                return;
            }
            db.addTrackLink(programmeId, trackId, track, addNextTrack);
        };
        
        db.retrieveTrackId(track, function(err, trackId) {
            if (err) {
                callback('Could not retrieve track id: ' + err);
                return;
            }            
            if (!trackId) {
                db.addTrack(track, addTrackLink);
                return;
            }
            addTrackLink(null, trackId);
        });        
    };
    
    db.addProgramme(programme, function(err, programmeId) {
        if (err) {
            callback('Could not add program: ' + err);
            return;
        }
        console.log('INFO: new programme id: ' + programmeId);
        addFirstTrack(programmeId, programme.tracks);
    });
}

function retrieveBbcListing(host, pathname, options, callback) {   
    console.log('INFO: retrieval: host: ' + host + ', pathname: ' + pathname);
    retrieveJson(host, pathname, function(programmeData, err) {
        if (!programmeData) {
            callback('Problem retrieving JSON: ' + err);
            return;
        }
        
        var versions = programmeData.programme.versions;
        var segmentsId = versions[versions.length - 1].pid;
        pathname = '/aps/programmes/' + segmentsId + '.json';
        
        retrieveJson(host, pathname, function(segmentsData) {
            if (!segmentsData) {
                callback('Problem retrieving segments JSON: ' + err);
                return;
            }
            
            processData(programmeData, segmentsData, host, callback);
        });
    });
}

function processData(programmeData, segmentData, host, callback) {
    var programme = programmeData.programme;
    var version = programme.versions[programme.versions.length - 1];
    
    var previous;
    if (programme.peers.previous) {
        previous = 'http://' + host + '/aps/programmes/' + programme.peers.previous.pid + '.json';
    }
    
    var next;
    if (programme.peers.next) {
        next = 'http://' + host + '/aps/programmes/' + programme.peers.next.pid + '.json';
    }
        
    var tracks = [];
    segmentData.version.segment_events.forEach(function(segment) {
        tracks.push({
            position: segment.position,
            offset: segment.version_offset,
            artist: segment.segment.artist,
            title: segment.segment.track_title,
            label: segment.segment.record_label,
            annotation: segment.title
        });
    });
       
    var result = {
        title: programme.display_title.title + ' - ' + programme.display_title.subtitle,
        timestamp: programme.first_broadcast_date,
        synopsis: programme.short_synopsis,
        duration: version.duration,
        category: programme.parent.programme.title,
        tracks: tracks,
        previous: previous,
        next: next
    };    
    
    callback(null, result);
}

function retrieveJson(host, path, callback) {
    var options = {
        host: host,
        path: path,
        port: 80,
        method: 'GET'
    };
    var req = http.request(options, function(res) {
        if (res.statusCode != 200) {        
            callback(null, 'Bad status code: ' + res.statusCode);
            res.on('data', function(chunk) {}); // seem to need to stop the code hanging
            res.on('end', function(chunk) {});
        }
        else {
            var data = '';
            res.on('data', function(chunk) {                
                if (chunk) {
                    data += chunk;
                }
            }).on('end', function() {
                // assume it's valid JSON
                callback(JSON.parse(data));
            });
        }           
    });
    req.on('error', function(e) {
        callback(null, 'request error: ' + e.message);
    });
    req.end();
}

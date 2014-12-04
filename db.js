var pg = require('pg');

var client;
var done;

/*
create table tracks
(
  id serial primary key,
  artist varchar(256),
  title varchar(256),
  label varchar(256)
);

create table programmes
(
  id serial primary key,
  timestamp timestamp with time zone,
  source_link varchar(256),
  title varchar(256),
  synopsis varchar(1024),
  duration int,
  category varchar(256)
);

create table track_lists
(
  programme_id int references programmes(id),
  track_id int references tracks(id),
  position int,
  programme_offset int,
  annotation varchar(256)
);

create table programme_chains
(
  programme_source varchar(256),
  timestamp timestamp with time zone,
  previous_source varchar(256),
  next_source varchar(256)
);
*/

exports.connect = function(connectionString, callback) {
    if (client) {
        console.log('WARN: pg connect called when client already exists');
        callback(true);
        return;
    }    
    pg.connect(connectionString, function(err, c, d) {
        if (err) {
            callback(false, err);
            return;
        }
        client = c;
        done = d;
        callback(true);
    });
}

exports.disconnect = function() {
    if (client) {
        done();
        done = undefined;
        client = undefined;
    }
    pg.end();
}

exports.retrieveProgrammeId = function(source, callback) {    
    if (!client) {
        callback('No pg client in retrieveProgrammeId call');
        return;
    }
    client.query('SELECT id, timestamp, source_link, title, synopsis, duration, category FROM programmes WHERE source_link = $1;', [ source ], function(err, result) {
        if (err) {
            callback('Problem retrieving programme for source: ' + source + ': ' + err);
            return;
        }
        if (result.rows.length == 0) {
            console.log('INFO: no programme entry for source: ' + source);
            callback();
            return;
        }
        if (result.rows.length > 1) {
            console.log('WARN: multiple programme entry found for source: ' + source);
        }        
        var row = result.rows[0];
        var programmeId = row.id;
        console.log('INFO: found programme id ' + programmeId + ' for source: ' + source);
        callback(null, programmeId);
    });
}

exports.retrieveChain = function(source, callback) {
    if (!client) {
        callback('No pg client in retrieveChain call');
        return;
    }
    console.log('INFO: retrieving chain for: ' + source);    
    client.query('SELECT timestamp, previous_source, next_source FROM programme_chains WHERE programme_source = $1;', [ source ], function(err, result) {
        if (err) {
            callback(err);
            return;
        }
        if (result.rows.length == 0) {
            console.log('INFO: no programme entry for source: ' + source);
            callback(null, null);
            return;
        }
        if (result.rows.length > 1) {
            console.log('WARN: multiple programme entry found for source: ' + source);
        }        
        var row = result.rows[0];
        callback(null, {
            source: source,
            timestamp: row.timestamp,
            previous: row.previous_source,
            next: row.next_source
        });
    });
}

exports.updateChain = function(programme, callback) {
    if (!client) {
        callback('No pg client in updateChain call');
        return;
    }    
    console.log('INFO: updating chain for: ' + programme.source);
    client.query(
        'UPDATE programme_chains SET previous_source = $2, next_source = $3, timestamp = $4 WHERE programme_source = $1;',
        [ programme.source, programme.previous, programme.next, programme.timestamp ],
        callback);
}

exports.addChain = function(programme, callback) {
    if (!client) {
        callback('No pg client in addChain call');
        return;
    }
    console.log('INFO: adding chain for: ' + programme.source);
    client.query(
        'INSERT INTO programme_chains VALUES ($1, $2, $3, $4);',
        [ programme.source, programme.timestamp, programme.previous, programme.next ],
        callback);
}

exports.deleteProgramme = function(programmeId, callback) {    
    if (!client) {
        callback('No pg client in deleteProgramme call');
        return;
    }
    
    console.log('INFO: deleting track_lists for programme id: ' + programmeId);
    client.query('DELETE FROM track_lists WHERE programme_id = $1', [ programmeId ], function(err, result) {
        if (err) {
            callback(err);
            return;
        }
        console.log('INFO: deleting programmes for programme id: ' + programmeId);
        client.query('DELETE FROM programmes WHERE id = $1', [ programmeId ], function(err, result) {
            callback(err);
        });
    });
}

exports.addProgramme = function(programme, callback) {    
    if (!client) {
        callback('No pg client in addProgramme call');
        return;
    }
    
    console.log('INFO: inserting programme for: ' + programme.source);
    client.query(
        'INSERT INTO programmes (timestamp, source_link, title, synopsis, duration, category) VALUES ($1, $2, $3, $4, $5, $6);',
        [ programme.timestamp, programme.source, programme.title, programme.synopsis, programme.duration, programme.category ],
        function(err, result) {
        if (err) {
            callback(err);
            return;
        }
        console.log('INFO: retrieving new programme id');
        client.query('SELECT id FROM programmes WHERE source_link = $1 ORDER BY id DESC;', [ programme.source ], function(err, result) {
            if (err) {
                callback(err);
                return;
            }
            console.log('INFO: got new programme id: ' + result.rows[0].id);
            callback(null, result.rows[0].id);
        });
    });
}

exports.retrieveTrackId = function(track, callback) {   
    if (!client) {
        callback('No pg client in retrieveTrackId call');
        return;
    }
    console.log('INFO: looking up track id for: ' + track.artist + ' - ' + track.title);
    client.query('SELECT id FROM tracks WHERE artist = $1 AND title = $2 ORDER BY id DESC;', [ track.artist, track.title ], function(err, result) {
        if (err) {
            callback('Problem looking up track: ' + err);
            return;
        }
        if (result.rows.length == 0) {
            // no error but no result
            console.log('INFO: no track id found, need to add');
            callback(null, null);
            return;
        }
        console.log('INFO: found track id: ' + result.rows[0].id);
        callback(null, result.rows[0].id);
    });    
}

exports.addTrack = function(track, callback) {   
    if (!client) {
        callback('No pg client in addTrack call');
        return;
    }

    console.log('INFO: inserting track for');
    client.query(
        'INSERT INTO tracks (artist, title, label) VALUES ($1, $2, $3);',
        [ track.artist, track.title, track.label ],
        function(err, result) {
        if (err) {
            callback(err);
            return;
        }
        console.log('INFO: retrieving new track id');
        client.query('SELECT id FROM tracks WHERE artist = $1 AND title = $2 AND (label IS NULL OR label = $3) ORDER BY id DESC;', [ track.artist, track.title, track.label ], function(err, result) {
            if (err) {
                callback(err);
                return;
            }
            console.log('INFO: got new track id: ' + result.rows[0].id);
            callback(null, result.rows[0].id);
        });
    });
}

exports.addTrackLink = function(programmeId, trackId, track, callback) {   
    if (!client) {
        callback('No pg client in addTrackLink call');
        return;
    }
    console.log('INFO: adding track link ' + programmeId + ' => ' + trackId);    
    client.query(
        'INSERT INTO track_lists VALUES ($1, $2, $3, $4, $5);',
        [ programmeId, trackId, track.position, track.offset, track.annotation ],
        function(err, result) {
        callback(err);
    });

}

exports.retrieveChainsToRetrieve = function(callback) {
    if (!client) {
        callback('No pg client in retrieveChainsToRetrieve call');
        return;
    }
    console.log('INFO: looking up chains to retrieve');
    client.query('select coalesce(next_source, programme_source) as source, case when next_source is null then 1 else 0 end as reretrieval from programme_chains pc1 where pc1.next_source is null or (select count(*) from programme_chains pc2 where pc2.programme_source = pc1.next_source) = 0;', [ ], function(err, result) {
        if (err) {
            callback('Problem looking up chains to retrieve: ' + err);
            return;
        }
        
        var chains = [];
        result.rows.forEach(function(row) {
            console.log('INFO: db chain source: ' + row.source + ', reretrieval: ' + row.reretrieval);
            chains.push({ source: row.source, reretrieval: row.reretrieval });
        });
        callback(null, chains);
    });
}

exports.retrieveAvailableChainListings = function(callback) {
    if (!client) {
        callback('No pg client in retrieveAvailableChainListings call');
        return;
    }
    console.log('INFO: looking up chains which we need to retrieve listings for');
    client.query("select pc.programme_source from programme_chains pc left outer join programmes p on pc.programme_source = p.source_link where p.source_link is null and pc.timestamp < (current_timestamp - interval '6 hours');", [ ], function(err, result) {
        if (err) {
            callback('Problem looking up chains to retrieve listings for: ' + err);
            return;
        }
        
        var sources = [];
        result.rows.forEach(function(row) {
            sources.push(row.programme_source);
        });
        callback(null, sources);
    });
}

exports.retrieveListings = function(callback) {
  console.log('INFO: retrieving listings');
  client.query("select id, timestamp, source_link, title, duration, category, synopsis from programmes;", [ ], function(err, result) {
    var results = [];
    result.rows.forEach(function(row) {
      results.push({
        id: row.id,
        timestamp: row.timestamp,
        source_link: row.source_link,
        title: row.title,
        duration: row.duration,
        category: row.category,
        synopsis: row.synopsis
      });
    });
    callback(results);
  });
}

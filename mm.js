

if (typeof(_MultiMongo_debug) === "undefined") {
    _MultiMongo_debug = false;
}

function _MultiMongo() {

    const _debug = function(...args) {
        if (_MultiMongo_debug) {
            print(...args);
        }
    };

    const _configColl = function(conn) {
        if (conn === undefined) {
            conn = db.getMongo();
        }
        return conn.getDB("config").mm.config;
    };

    const _statusColl = function(conn) {
        if (conn === undefined) {
            conn = db.getMongo();
        }
        return conn.getDB("config").mm.status;
    };

    const _cmpOpTime = function (a, b) {
        // i considered sorted by `wall`, but that seems impossibly dodgy
        if (a.t < b.t) {
            return -1;
        }
        if (a.t > b.t) {
            return 1;
        }
        if (a.ts < b.ts) {
            return -1;
        }
        if (a.ts > b.ts) {
            return 1;
        }
        return 0;
    };

    const _maxOpTime = function (a, b) {
        if (a && !b) {
            return a;
        }
        if (!a && b) {
            return b;
        }
        if (!a && !b) {
            return undefined;
        }
        return _cmpOpTime(a, b) > 0 ? a : b;
    };

    const _grabRelevantOplog = function(conn, t, ts) {
        const oplogEntries = conn.getDB("local").oplog.rs.find( { op: { $nin: [ "n", "c" ] }, t: { $gte: t }, ts: { $gte: ts }, ns: { $not: { $regex: "^admin\." } } } ).toArray();
        // if the first entry exactly matches t,ts, then throw it away (we have to use gte because of how t and ts are separate)
        if (oplogEntries.length > 0 && _cmpOpTime(oplogEntries[0], {t,ts}) === 0) {
            oplogEntries.shift();
        }
        var ignore = false;
        return oplogEntries.filter(oplogEntry => {
            var shouldInclude = true;
            if (oplogEntry.op === "i" && oplogEntry.ns === "config.mm._meta") {
                ignore = true;
            }
            if (ignore) {
                shouldInclude = false;
            }
            if (oplogEntry.ns.startsWith("config.")) {
                shouldInclude = false;
            }
            if (oplogEntry.op === "d" && oplogEntry.ns === "config.mm._meta") {
                ignore = false;
            }
            return shouldInclude;
        });
    };

    const _getOplogDoc = function(oplogEntry) {
        // Blergh
        return tojson({ ns: oplogEntry.ns, id: oplogEntry.o._id });
    };

    const _getOplogOpTime = function(oplogEntry) {
        if ( ! oplogEntry) {
            return { t: NumberLong(-1), ts: Timestamp() };
        }
        return { t: oplogEntry.t, ts: oplogEntry.ts };
    };

    const _getMinvalidFromOplogTail = function(conn) {
        if (conn === undefined) {
            conn = db.getMongo();
        }
        const finalOplogCursor = conn.getDB("local").oplog.rs.find().limit(1).sort({$natural:-1});
        if ( ! finalOplogCursor.hasNext()) {
            return { t: NumberLong(-1), ts: Timestamp() };
        }
        return _getOplogOpTime(finalOplogCursor.next());
    };

    const _generatePreimageQuery = function(oplogEntry) {
        return { "_id.nsUUID": oplogEntry.ui, "_id.ts": oplogEntry.ts, "preImage._id": oplogEntry.o2 ? oplogEntry.o2._id : oplogEntry.o._id, $comment: { ns: oplogEntry.ns } };
    };

    const _applyOplog = function(peerName, oplogEntries) {
        // strip the `ui` field from the oplog entries, and just keep the `ns` field
        // since the collections in each peer with the same name will (by design) have different uuids
        oplogEntries = oplogEntries.map(oplogEntry => {
            delete oplogEntry.ui;
            return oplogEntry;
        });

        _debug(tojson(oplogEntries));

        if (oplogEntries.length > 0) {
            // FIXME: send batches up to ~16Mb
            // i tried setting `oplogApplicationMode: "Secondary"` (instead of the default `"ApplyOps"`), and all the other possible modes, it didn't seem to help with anything i needed
            //assert.commandWorked(db.adminCommand( { appendOplogNote: 1, data: { oplogEntries: oplogEntries } } ));
            //assert.commandWorked(db.adminCommand( { appendOplogNote: 1, data: { oplogEntries: Array.from(oplogDocsPeerOnly) } } ));
            //_debug(tojson(db.adminCommand( { appendOplogNote: 1, data: { oplogEntries: Array.from(oplogDocsPeerOnly) } } )));
            //assert.commandWorked(db.adminCommand( { applyOps: oplogEntries } ));
            //_debug(tojson(db.adminCommand( { applyOps: oplogEntries } )));

            // It's irritating that the only way to signal "ignore these ops" has to involve doing actual writes.
            // These could just as easily be noops, but applyOps doesn't propagate 'n' ops.
            // appendOplogNote before and after isn't part of the global lock that applyOps takes, so risks masking ops that shouldn't be ignored.
            // "c" ops are no good, because there's a whitelist of allowed commands, and they all have side effects (including writes).
            // Doing "noop writes" (eg. deleting a known non-existent doc) are no good because they don't get oplogged.
            // Oplog entry fields can't be used, because there's a whitelist of allowed fields, and the potentially useful ones (fromMigrate, prevOpTime, _id) aren't propagated by applyOps.
            // So here we are.
            oplogEntries = [
                    {
                        op: "i",
                        ns: "config.mm._meta",
                        o: { _id: peerName }
                    },
                    ...oplogEntries,
                    {
                        op: "d",
                        ns: "config.mm._meta",
                        o: { _id: peerName }
                    },
                ];

            const res = db.adminCommand( { applyOps: oplogEntries } );
            _debug(tojson(res));
            assert.commandWorked(res);

            //_debug(tojson(db.runCommand( { applyOps: oplogEntries } )));
            //assert.commandWorked(db.adminCommand( { applyOps: oplogEntries, oplogApplicationMode: "Secondary" } ));
            //_debug(tojson(db.adminCommand( { applyOps: oplogEntries, oplogApplicationMode: "Secondary" } )));
        }
    };

    const _queryPreimages = function(conn, preimageQueries) {
        _debug(tojson(preimageQueries));
        var predicates = [];
        var nsUUIDs = {};
        for (let oplogDoc in preimageQueries) {
            if (preimageQueries[oplogDoc] !== null) {
                nsUUIDs[preimageQueries[oplogDoc]["_id.nsUUID"]] = preimageQueries[oplogDoc]["$comment"].ns;
                predicates.push(preimageQueries[oplogDoc]);
            }
        }
        _debug(tojson(predicates));
        _debug(tojson(nsUUIDs));
        if (predicates.length === 0) {
            return [];
        }

        var preimages = conn.getDB("config").system.preimages.find({$or: predicates}).toArray();
        _debug(tojson(preimages));

        //return [];
        return preimages.map(preimage => {
            // return an oplog entry which sets the doc to its preimage
            // even though we have the nsUUID in preimage._id.nsUUID, do not set the ui field to it
            // this is because if the preimage has come from the peer, then the uuid will be for its ns, not ours
            return {
                op: "u",
                ns: nsUUIDs[preimage._id.nsUUID],
                o: preimage.preImage,
                o2: { _id: preimage.preImage._id },
            };
        });
    };

	return {
		debug: function(d) {
			_MultiMongo_debug = d;
		},

		reload: function() {
			load("mm.js");
		},

		reset: function(confirm) {
			if ( ! confirm || confirm.confirm !== "yes") {
				print("To reset multimongo state, run mm.reset({confirm:'yes'})");
				return;
			}
			_configColl().drop();
			_statusColl().drop();
		},

		conf: function() {
			var configDocCursor = _configColl().find({_id:"config"});
			if ( ! configDocCursor.hasNext()) {
				_configColl().insert({_id:"config", peers: []});
				configDocCursor = _configColl().find({_id:"config"});
			}
			const configDoc = configDocCursor.next();
			return configDoc;
		},

		status: function() {
			const configDoc = this.conf();
			printjson(configDoc);

			const peers = configDoc.peers;
			_statusColl().find().forEach(printjsononeline);
		},

		setup: function() {
            db.getSiblingDB("config").createCollection("mm._meta");
            // enable preimages on all collections
			const myDBNames = db.getMongo().getDBNames().filter(n => !['admin','local','config'].includes(n));
			for (var myDBName of myDBNames) {
				var collNames = db.getMongo().getDB(myDBName).getCollectionNames();
				for (var collName of collNames) {
					assert.commandWorked(db.getSiblingDB(myDBName).runCommand( { collMod: collName, changeStreamPreAndPostImages: { enabled: true } } ));
				}
			}
		},

		addPeer: function(newPeer) {
			// do an "initial sync" _onto_ the new peer (ie. _push_ my data (and sync status) onto it)

			if (newPeer === undefined || newPeer === '' || typeof(newPeer) !== 'string') {
				print("Usage: mm.addPeer('newpeer:port')");
				return;
			}

			_debug("Step 0: Connecting to new peer " + newPeer);
			var peer = new Mongo(newPeer);
			_debug(peer);


			_debug("Step 0b: Checking peer connection");
			const me = db.getMongo().host;
			const myId = db.runCommand( { _isSelf: 1 } ).id;
			const peerId = peer.getDB("admin").runCommand( { _isSelf: 1 } ).id;
			_debug(myId, me);
			_debug(peerId, peer.host);
			if (peerId.equals(myId)) {
				throw "Error: peer must not be self";
			}

			_debug("Step 1: Grab a bunch of stuff"); // "pseudo-atomically"

			_debug("Step 1a: Grabbing my config");
			const conf = this.conf();
			var newPeerConf = this.conf();
			newPeerConf.peers.push(me);
			var myNewConf = this.conf();
			myNewConf.peers.push(newPeer);

			_debug("Step 1b: Grabbing my peer minvalids and my minvalid");
			var myPeerMinvalids = _statusColl().find().toArray();
			const myMinvalid = _getMinvalidFromOplogTail();
			myPeerMinvalids.push( { _id: me, minvalid: myMinvalid } );

			_debug("Step 1c: Grabbing my list of dbs, collections, and indexes");
			const myDBNames = db.getMongo().getDBNames().filter(n => !['admin','local','config'].includes(n));
			var everything = {};
			for (var myDBName of myDBNames) {
				var collNames = db.getMongo().getDB(myDBName).getCollectionNames();
				everything[myDBName] = {};
				for (var collName of collNames) {
					everything[myDBName][collName] = db.getSiblingDB(myDBName).getCollection(collName).getIndexes();
				}
			}
			_debug(tojson(everything));

			_debug("Step 1d: Connecting to existing peers");
			const peerConns = conf.peers.map(otherPeer => new Mongo(otherPeer));


			_debug("Step 2: Clearing out everything on the new peer");
			peer.getDBNames().filter(n => !['admin','local','config'].includes(n)).forEach(dname => peer.getDB(dname).dropDatabase());
			_configColl(peer).drop();
			_statusColl(peer).drop();


			_debug("Step 3: Copying all my data to the new peer");
			// blergh, fml
			for (var dbName in everything) {
				_debug("  Cloning db " + dbName);
				for (var collName in everything[dbName]) {
					_debug("    Cloning coll " + dbName + "." + collName);
					assert.commandWorked(peer.getDB(dbName).createCollection(collName, { changeStreamPreAndPostImages: { enabled: true } }));
					db.getMongo().getDB(dbName).getCollection(collName).find().forEach(doc => peer.getDB(dbName).getCollection(collName).insert(doc));
					_debug("    Building indexes for coll " + dbName + "." + collName);
					peer.getDB(dbName).runCommand( { createIndexes: collName, indexes: everything[dbName][collName] } );
				}
			}


			_debug("Step 4: Setting the new peer's statuses");  // to the minvalids grabbed above in step 1b
			_statusColl(peer).insertMany(myPeerMinvalids);


			_debug("Step 5: Setting the new peer's config");  // same as mine, but with me in there as well
			_configColl(peer).insert(newPeerConf);


			_debug("Step 6: Grabbing the new peer's minvalid");
			const newPeerMinvalid = _getMinvalidFromOplogTail(peer);


			_debug("Step 7: Setting my status for the new peer");  // set it to the value grabbed above in step 6
			const newPeerStatus = {_id: newPeer, minvalid: newPeerMinvalid };
			_statusColl().insert(newPeerStatus);


			_debug("Step 8: Doing step 7 for all my peers");
			peerConns.forEach(otherPeer => _statusColl(otherPeer).insert(newPeerStatus));


			_debug("Step 9: Adding the new peer to my config");
			_configColl().update({_id:"config"}, myNewConf);


			_debug("Step 10: Adding the new peer to the config of all my peers");
			peerConns.forEach(otherPeer => _configColl(otherPeer).update({_id:"config"}, myNewConf));


			_debug("Done!");
		},

		removePeer: function(newPeer) {
			print("TODO");
		},

		sync: function() {
			this.conf().peers.forEach(peerName => this.syncPeer(peerName));
		},

		syncPeer: function(peerName) {
			// FIXME: is the below actually correct when there are multiple peers?  is doing all the peers and their oplogs together all at once merely more efficient?  need to fully think this through...
			//
			// For each peer:
			//    Step 0: get the minvalid for this peer from my status
			//    Step 1: grab my (relevant) oplog entries from that time onwards
			//       Step 1b: build the set of affected docs (identified as as db.coll._id)
			//    Step 2: grab the peer's (relevant) oplog entries from that time onwards
			//       Step 2b: build the set of affected docs (identified as as db.coll._id)
			//    Step 3: split 2b into those that are also in 1b (ie. the intersection), and the rest (peer only) (don't need to do the "me only" ones - they've already been applied!)
			//    Step 4: the oplog entries for the rest can just be blindly applied
			//             except we need some way of flagging the applyOps (either at the top level, or for each op) so that it will be ignored by other syncs.
			//    Step 5: the oplog entries for those in common (the intersection) need special handling.  for each affected id, construct the applyops array by:
			//       Step 5a: reset the doc to its preimage.
			//          Step 5a1: to get the preimage, search my config.system.preimages for _id matching the nsUUID + ts (of the first of MY oplog entries for this doc, ie. the first time i modified the doc).  usually there will be just one result, but there could be more (with incrementing _id.applyOpsIndex values).  iterate them to find the one with preImage._id that matches this doc.
			//                    FIXME: this is wrong.  the right way is, check if the earliest oplog timestamp for this doc is in my oplog, or the peer's oplog.  if mine, then grab the preimage from me; if the peer, then grab it from the peer's config.system.preimages.
			//       Step 5b: follow this by a merged array of oplog entries for this doc, sorted by t,ts.
			//                 need some way of flagging the applyOps (either at the top level, or for each op) so that it will be ignored by other syncs.
			//    Step 6: apply all the ops.  maybe try to spread out the ops for each doc across multiple applyops calls?  doing a separate applyops for each doc will preclude any parallelism at all.  maybe generate a giant applyops list, across all the docs (affected and not affected), sorted by time (t,ts), and then batch that up?
			//    Step 7: update my status minvalid for this peer to be the {t,ts} from the later of: my final oplog entry (grabbed in step 1), or the peer's final oplog entry (grabbed in step 2)

			// > dbs.local.oplog.rs.find({op:{$nin:["n","c"]}, ns:{$not:{$regex:"^(admin|config)\."}}}).forEach(printjsononeline)

			const peerStatus = _statusColl().find({_id:peerName}).next();
			const peerMinvalid = peerStatus.minvalid;
			_debug("Syncing from peer", tojsononeline(peerStatus));
			const peerConn = new Mongo(peerName);


			_debug("  Step 1: Grabbing my oplog");
            const myOplog = _grabRelevantOplog(db.getMongo(), peerMinvalid.t, peerMinvalid.ts);

            const myOplogDocs = new Set();
            const myOplogDocsEarliest = {};
            for (var oplogEntry of myOplog) {
                const oplogDoc = _getOplogDoc(oplogEntry);
                if ( ! myOplogDocs.has(oplogDoc)) {
                    myOplogDocs.add(oplogDoc);
                    myOplogDocsEarliest[oplogDoc] = _getOplogOpTime(oplogEntry);
                }
            }
			_debug("          " + myOplogDocs.size + " docs in " + myOplog.length + " entries");
            _debug(tojson(myOplogDocsEarliest));

            const myFinalOplogEntry = myOplog[myOplog.length - 1];
            const myFinalOplogTime = (function () {
                if (myFinalOplogEntry) {
                    const res = { t: myFinalOplogEntry.t, ts: myFinalOplogEntry.ts };
                    _debug("          Ends at " + tojson(res));
                    return res;
                }
            })();


			_debug("  Step 2: Grabbing peer's oplog");
            const peerOplog = _grabRelevantOplog(peerConn, peerMinvalid.t, peerMinvalid.ts);

            const peerOplogDocs = new Set();
            const peerOplogDocsEarliest = {};
            for (var oplogEntry of peerOplog) {
                const oplogDoc = _getOplogDoc(oplogEntry);
                if ( ! peerOplogDocs.has(oplogDoc)) {
                    peerOplogDocs.add(oplogDoc);
                    peerOplogDocsEarliest[oplogDoc] = _getOplogOpTime(oplogEntry);
                }
            }
			_debug("          " + peerOplogDocs.size + " docs in " + peerOplog.length + " entries");
            _debug(tojson(peerOplogDocsEarliest));

            const peerFinalOplogEntry = peerOplog[peerOplog.length - 1];
            const peerFinalOplogTime = (function () {
                if (peerFinalOplogEntry) {
                    const res = { t: peerFinalOplogEntry.t, ts: peerFinalOplogEntry.ts };
                    _debug("          Ends at " + tojson(res));
                    return res;
                }
            })();
            //if (peerOplog.length === 0) {
            //    // nothing's happened on the peer, so nothing to do
            //    // WRONG: still need to update the minvalid time.  so just continue through the motions.
            //    return;
            //}


            //_debug(tojsononeline(Array.from(myOplogDocs)));
            //_debug(tojsononeline(Array.from(peerOplogDocs)));
			_debug("  Step 3: Splitting oplogs");
            const oplogDocsPeerOnly = new Set();
            const oplogDocsCommon = new Set();
            const oplogDocsMyOnly = new Set();
            for (var oplogDoc of peerOplogDocs) {
                //_debug(tojsononeline(oplogDoc));
                if (myOplogDocs.has(oplogDoc)) {
                    //_debug("yes");
                    oplogDocsCommon.add(oplogDoc);
                } else {
                    //_debug("no");
                    oplogDocsPeerOnly.add(oplogDoc);
                }
            }
            for (var oplogDoc of myOplogDocs) {
                if ( ! peerOplogDocs.has(oplogDoc)) {
                    oplogDocsMyOnly.add(oplogDoc);
                }
            }
			_debug("          " + oplogDocsCommon.size + " docs in common, " + oplogDocsPeerOnly.size + " peer-only docs, " + oplogDocsMyOnly.size + " me-only docs");


			_debug("  Step 4: Applying oplogs for unaffected docs");
            var onlyOplog = [];
            for (var oplogEntry of peerOplog) {
                if (oplogDocsPeerOnly.has(_getOplogDoc(oplogEntry))) {
                    onlyOplog.push(oplogEntry);
                }
            }
            onlyOplog.sort(_cmpOpTime);
            _applyOplog(peerName, onlyOplog);


			_debug("  Step 5: Generating oplogs for affected docs");
            // first, go through and get the earliest optime of each affected doc in my oplog - DONE ABOVE
            // then the same for the peer's oplog - DONE ABOVE
            // then get a list of docs which are earliest in my oplog, and the others which are earliest in the peer's oplog
            // then query my config.system.preimages for all the docs (based on nsUUID+ts(the earliest ts)+preImage._id) in my list
            // and query the peer's config.system.preimages for all the docs (based on nsUUID+ts(the earliest ts)+preImage._id) in the peer's list
            // create an applyops array which sets these preimage doc values
            // then append the sorted commonOplog entries to this
            const commonOplog = [];
            const peerPreimagesQueries = {};
            for (var oplogEntry of peerOplog) {
                if (oplogDocsCommon.has(_getOplogDoc(oplogEntry))) {
                    commonOplog.push(oplogEntry);

                    const oplogDoc = _getOplogDoc(oplogEntry);
                    if ( ! (oplogDoc in peerPreimagesQueries) ) {
                        const myEarliestOpTime = myOplogDocsEarliest[oplogDoc];
                        const peerEarliestOpTime = peerOplogDocsEarliest[oplogDoc];
                        if (_cmpOpTime(myEarliestOpTime, peerEarliestOpTime) <= 0) {
                            // myEarliestOpTime <= peerEarliestOpTime
                            // so this one is mine
                            // but this is the peer oplog.  so just make sure we don't try again.
                            peerPreimagesQueries[oplogDoc] = null; // stops us from wasting time on subsequent oplog entries for this oplogDoc, since they must be even later in time
                        } else {
                            // myEarliestOpTime > peerEarliestOpTime
                            // so this one is the peer's
                            peerPreimagesQueries[oplogDoc] = _generatePreimageQuery(oplogEntry);
                        }
                    }
                }
            }
            // need to also iterate myOplog, not just peerOplog
            const myPreimagesQueries = {};
            for (var oplogEntry of myOplog) {
                if (oplogDocsCommon.has(_getOplogDoc(oplogEntry))) {
                    commonOplog.push(oplogEntry);

                    const oplogDoc = _getOplogDoc(oplogEntry);
                    if ( ! (oplogDoc in myPreimagesQueries) ) {
                        const myEarliestOpTime = myOplogDocsEarliest[oplogDoc];
                        const peerEarliestOpTime = peerOplogDocsEarliest[oplogDoc];
                        if (_cmpOpTime(myEarliestOpTime, peerEarliestOpTime) <= 0) {
                            // myEarliestOpTime <= peerEarliestOpTime
                            // so this one is mine
                            myPreimagesQueries[oplogDoc] = _generatePreimageQuery(oplogEntry);
                        } else {
                            // myEarliestOpTime > peerEarliestOpTime
                            // so this one is the peer's
                            // but this is my oplog.  so just make sure we don't try again.
                            myPreimagesQueries[oplogDoc] = null; // stops us from wasting time on subsequent oplog entries for this oplogDoc, since they must be even later in time
                        }
                    }
                }
            }
            commonOplog.sort(_cmpOpTime);

            // actually run the preimages queries, and put the results into preimageOps in the appropriate format
            const myPreimageOps = _queryPreimages(db.getMongo(), myPreimagesQueries);
            _debug("myPreimageOps", tojson(myPreimageOps));
            const peerPreimageOps = _queryPreimages(peerConn, peerPreimagesQueries);
            _debug("peerPreimageOps", tojson(peerPreimageOps));
            const preimageOps = [ ...myPreimageOps, ...peerPreimageOps ];


			_debug("  Step 6: Applying oplogs for affected docs");
            _applyOplog(peerName, [ ...preimageOps, ...commonOplog ]);


            // FIXME: strictly speaking, the status for each peer should store the minvalid optime for the peer's oplog (this is what's in there now), as well as the minvalid optime for MY OPLOG.
            // conflating these two things like this isn't ideal, even if it works in the regular case, because it further enforces the "single timeline".  (even though merging the oplogs requires that anyway.)
            const finalOplogTime = _maxOpTime(myFinalOplogTime, peerFinalOplogTime);
            if (finalOplogTime) {
                _debug("  Step 7: Updating my status minvalid for this peer to " + tojson(finalOplogTime));
                _statusColl().update( { _id: peerName }, { $set: { minvalid: finalOplogTime } } );
            }


			_debug("Done!");
		},
	};
}

var mm = new _MultiMongo();


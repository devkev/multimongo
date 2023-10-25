
function _MultiMongo() {
	return {
		reload: function() {
			load("mm.js");
		},

		_configColl: function(conn) {
			if (conn === undefined) {
				conn = db.getMongo();
			}
			return conn.getDB("config").mm.config;
		},

		_statusColl: function(conn) {
			if (conn === undefined) {
				conn = db.getMongo();
			}
			return conn.getDB("config").mm.status;
		},

		_getMinvalidFromOplogTail: function(conn) {
			if (conn === undefined) {
				conn = db.getMongo();
			}
			const finalOplogCursor = conn.getDB("local").oplog.rs.find().limit(1).sort({$natural:-1});
			if ( ! finalOplogCursor.hasNext()) {
				return { t: NumberLong(-1), ts: Timestamp() };
			}
			const finalOplog = finalOplogCursor.next();
			return { t: finalOplog.t, ts: finalOplog.ts };
		},

		reset: function(confirm) {
			if ( ! confirm || confirm.confirm !== "yes") {
				print("To reset multimongo state, run mm.reset({confirm:'yes'})");
				return;
			}
			this._configColl().drop();
			this._statusColl().drop();
		},

		conf: function() {
			var configDocCursor = this._configColl().find({_id:"config"});
			if ( ! configDocCursor.hasNext()) {
				this._configColl().insert({_id:"config", peers: []});
				configDocCursor = this._configColl().find({_id:"config"});
			}
			const configDoc = configDocCursor.next();
			return configDoc;
		},

		status: function() {
			const configDoc = this.conf();
			printjson(configDoc);

			const peers = configDoc.peers;
			this._statusColl().find().forEach(printjsononeline);
		},

		addPeer: function(newPeer) {
			// do an "initial sync" _onto_ the new peer (ie. _push_ my data (and sync status) onto it)

			if (newPeer === undefined || newPeer === '' || typeof(newPeer) !== 'string') {
				print("Usage: mm.addPeer('newpeer:port')");
				return;
			}

			print("Step 0: Connecting to new peer " + newPeer);
			var peer = new Mongo(newPeer);
			print(peer);


			print("Step 0b: Checking peer connection");
			const me = db.getMongo().host;
			const myId = db.runCommand( { _isSelf: 1 } ).id;
			const peerId = peer.getDB("admin").runCommand( { _isSelf: 1 } ).id;
			print(myId, me);
			print(peerId, peer.host);
			if (peerId.equals(myId)) {
				throw "Error: peer must not be self";
			}

			print("Step 1: Grab a bunch of stuff"); // "pseudo-atomically"

			print("Step 1a: Grabbing my config");
			const conf = this.conf();
			var newPeerConf = this.conf();
			newPeerConf.peers.push(me);
			var myNewConf = this.conf();
			myNewConf.peers.push(newPeer);

			print("Step 1b: Grabbing my peer minvalids and my minvalid");
			var myPeerMinvalids = this._statusColl().find().toArray();
			const myMinvalid = this._getMinvalidFromOplogTail();
			myPeerMinvalids.push( { _id: me, minvalid: myMinvalid } );

			print("Step 1c: Grabbing my list of dbs, collections, and indexes");
			const myDBNames = db.getMongo().getDBNames().filter(n => !['admin','local','config'].includes(n));
			var everything = {};
			for (var myDBName of myDBNames) {
				var collNames = db.getMongo().getDB(myDBName).getCollectionNames();
				everything[myDBName] = {};
				for (var collName of collNames) {
					everything[myDBName][collName] = db.getSiblingDB(myDBName).getCollection(collName).getIndexes();
				}
			}
			printjson(everything);

			print("Step 1d: Connecting to existing peers");
			const peerConns = conf.peers.map(otherPeer => new Mongo(otherPeer));


			print("Step 2: Clearing out everything on the new peer");
			peer.getDBNames().filter(n => !['admin','local','config'].includes(n)).forEach(dname => peer.getDB(dname).dropDatabase());
			this._configColl(peer).drop();
			this._statusColl(peer).drop();


			print("Step 3: Copying all my data to the new peer");
			// blergh, fml
			for (var dbName in everything) {
				print("  Cloning db " + dbName);
				for (var collName in everything[dbName]) {
					print("    Cloning coll " + dbName + "." + collName);
					assert.commandWorked(peer.getDB(dbName).createCollection(collName, { changeStreamPreAndPostImages: { enabled: true } }));
					db.getMongo().getDB(dbName).getCollection(collName).find().forEach(doc => peer.getDB(dbName).getCollection(collName).insert(doc));
					print("    Building indexes for coll " + dbName + "." + collName);
					peer.getDB(dbName).runCommand( { createIndexes: collName, indexes: everything[dbName][collName] } );
				}
			}


			print("Step 4: Setting the new peer's statuses");  // to the minvalids grabbed above in step 1b
			this._statusColl(peer).insertMany(myPeerMinvalids);


			print("Step 5: Setting the new peer's config");  // same as mine, but with me in there as well
			this._configColl(peer).insert(newPeerConf);


			print("Step 6: Grabbing the new peer's minvalid");
			const newPeerMinvalid = this._getMinvalidFromOplogTail(peer);


			print("Step 7: Setting my status for the new peer");  // set it to the value grabbed above in step 6
			const newPeerStatus = {_id: newPeer, minvalid: newPeerMinvalid };
			this._statusColl().insert(newPeerStatus);


			print("Step 8: Doing step 7 for all my peers");
			peerConns.forEach(otherPeer => this._statusColl(otherPeer).insert(newPeerStatus));


			print("Step 9: Adding the new peer to my config");
			this._configColl().update({_id:"config"}, myNewConf);


			print("Step 10: Adding the new peer to the config of all my peers");
			peerConns.forEach(otherPeer => this._configColl(otherPeer).update({_id:"config"}, myNewConf));


			print("Done!");
		},

		removePeer: function(newPeer) {
			print("TODO");
		},

		sync: function() {
			print("TODO");

			// FIXME: is the below actually correct when there are multiple peers?  is doing all the peers and their oplogs together all at once merely more efficient?  need to think this through...
			//
			// For each peer:
			//    Step 0: get the minvalid for this peer from my status
			//    Step 1: grab my (relevant) oplog entries from that time onwards
			//       Step 1b: build the set of affected docs (identified as as db.coll._id)
			//    Step 2: grab the peer's (relevant) oplog entries from that time onwards
			//       Step 2b: build the set of affected docs (identified as as db.coll._id)
			//    Step 3: split 2b into those that are also in 1b (ie. the union), and the rest
			//    Step 4: the oplog entries for the rest can just be blindly applied
			//             FIXME: except we may need some way of flagging  the applyOps (either at the top level, or for each op) so that it will be ignored by other syncs.  currently we just ignore all applyOps, which solves the problem, but only by restricting clients by not allowing them to use applyOps.
			//    Step 5: the oplog entries for those in common (the union) need special handling.  for each affected id, construct the applyops array by:
			//       Step 5a: reset the doc to its preimage.
			//          Step 5a1: to get the preimage, search my config.system.preimages for _id matching the nsUUID + ts (of the first of MY oplog entries for this doc, ie. the first time i modified the doc).  usually there will be just one result, but there could be more (with incrementing _id.applyOpsIndex values).  iterate them to find the one with preImage._id that matches this doc.
			//                    FIXME: this is wrong.  the right way is, check if the earliest oplog timestamp for this doc is in my oplog, or the peer's oplog.  if mine, then grab the preimage from me; if the peer, then grab it from the peer's config.system.preimages.
			//       Step 5b: follow this by a merged array of oplog entries for this doc, sorted by t,ts.
			//                 FIXME: ideally, need some way of flagging the applyOps (either at the top level, or for each op) so that it will be ignored by other syncs.  currently we just ignore all applyOps, which solves the problem, but only by restricting clients by not allowing them to use applyOps.
			//    Step 6: apply all the ops.  maybe try to spread out the ops for each doc across multiple applyops calls?  doing a separate applyops for each doc will preclude any parallelism at all.  maybe generate a giant applyops list, across all the docs (affected and not affected), sorted by time (t,ts), and then batch that up?
			//    Step 7: update my status minvalid for this peer to be the {t,ts} from the later of: my final oplog entry (grabbed in step 1), or the peer's final oplog entry (grabbed in step 2)

			// > dbs.local.oplog.rs.find({op:{$nin:["n","c"]}, ns:{$not:{$regex:"^(admin|config)\."}}}).forEach(printjsononeline)
		},
	};
}

var mm = new _MultiMongo();


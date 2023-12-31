MultiMongo: Bidirectional synchronisation of multiple separate MongoDB clusters
===============================================================================

![Demo showing basic usage of MultiMongo](multimongo-demo.gif)

Yes, this has a whole lot of limitations.  Mostly this thing is intended for simple, lightweight, human-timescale applications - the actual motivation is a personal todo system which supports writes while offline, and syncs up when online again.  So it's fine to have "downsides" like clocks synced only with NTP (rather than atomic clocks like Spanner), inconsistent data in between syncs, naive last-write-wins conflict resolution, etc.

I looked into Realm, but it has no Python API (since it's targeted at mobile devices, not laptops) and Grainite, but it seemed like a complicated sledgehammer for cracking this nut.  In the absence of such syncing frameworks, the usual approach is to either just accept online-only usage (perhaps with a read-only local cache), or for the application to handle all the syncing logic, ie. remembering the user operations, when they were done, and then reconciling them when back online.  That's frustrating because it's basically reinventing the oplog.  So I figured, why not just use the oplog for this purpose?

The main wrinkle is needing document preimages, to be able to replay merged oplogs from the time of the previous sync.  I used the changestream preimage mechanism for this (even though there's annoyingly no way to make it enabled on all collections by default).  It doesn't even need any changestreams, querying `config.system.preimages` directly is fine.  The default expiry policy of "alongside the corresponding oplog entries" is fine, since I'm relying on the oplog anyway, so I just need to make sure that the oplog is large enough.

It should also be possible to use snapshot reads with `atClusterTime` to get the preimage, which would better leverage WiredTiger and avoid the double-writes involved in maintaining `config.system.preimages`.  However the only client-available retention policy for this is `minSnapshotHistoryWindowInSeconds`, which doesn't align well with this use case.  Better would be a client-accessible way of advancing the oldest history timestamp, much like how the majority commit point is used for non-`atClusterTime` snapshot reads.  Then it could be advanced to the oldest known sync minvalid across all the peers.

Of course, even though I run mm.sync() manually in the demo, the idea is that each peer will just run it every ~5 secs (or whatever) in a background process.

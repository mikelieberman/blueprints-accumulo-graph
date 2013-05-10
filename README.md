blueprints-accumulo-graph
=========================

Implementation of the Tinkerpop Blueprints API backed by Accumulo.
The graph is stored in a single table in Accumulo.  This adapter has
support for key/value indexing and some performance tweaks.  If
indexing is enabled, the index is stored in a separate table.
Everything is serialized using Kryo.

The graph is stored in a single table with the following schema.

Row         CF          CQ          Val         Purpose
---         --          --          ---         -------

MVERTEX     [v id]      -           -           Global vertex list
[v id]      MVERTEX     -           -           Vertex id
[v id]      EOUT        [e id]      [e label]   Vertex out-edge
[v id]      EIN         [e id]      [e label]   Vertex in-edge

MEDGE       [e id]      -           -           Global edge list
[e id]      MEDGE       [e label]   -           Edge id
[e id]      VOUT        [v id]      -           Edge out-vertex
[e id]      VIN         [v id]      -           Edge in-vertex

[v/e id]    PROP        [p name]    [p val]     Element property


If the index table is enabled, it has the following schema.

Row         CF          CQ          Val         Purpose
---         --          --          ---         -------

PVLIST      [p name]    -           -           Property list
[p name]    [p val]     [v/e id]    -           Property index


I have not done serious performance testing, but it seems okay. :)


How to use it
=============

Create an instance of AccumuloGraphOptions, which allows control over
various features implemented by this adapter using get/set methods.
Then create an instance of AccumuloGraph and pass in the options
object.

Options are as follows:

 - Connector info (required): Set zookeepers, instance name, username,
   password.  Essentially the values you need to connect to Accumulo.
   Alternatively, pass in an Accumulo Connector object which represents
   the connection.

 - Graph table (required): Where to store the graph.

 - Index table (optional): Where to store the key/value index.

 - Autoflush (optional, default true): Immediately flush changes to
   Accumulo, rather than waiting for performance reasons.  If
   disabled, may cause timing issues (see caveats).

 - Return removed property values (optional, default true): The
   removeProperty method specifies that the value of the removed
   property is returned.  This potentially requires another read from
   Accumulo.  If you don't care what is returned, disable this
   to speed things up.


Caveats
=======

There are definitely bugs.

Timing issues: There may be a lag time between when you add a
vertex/edge, set their properties, etc. and when it is reflected in
the backing Accumulo table.  This is done for performance reasons, but
as a result, if you set values and then immediately read them back,
the results may be inconsistent.  The same holds for key/value
indexes.  This isn't a problem if you're doing things like bulk loads,
or using the graph as read-only, but otherwise it may be problematic.
If this is an issue, this can be mitigated somewhat using the
autoflush option, where changes are flushed immediately to Accumulo,
at the cost of write performance.  I have tried to reduce these timing
issues as much as possible, but there may still be issues with this,
and it needs more testing.


Todo
====
- Read-only usage.  This will enforce only read operations, and would
  allow caching strategies, and avoid timing issues.

- Element/property cache, to increase performance for read-only
  usage.

- Regular-style indexes, in addition to key/value index.

- Tuned querying.

- Bulk loading of graph elements.

- Detailed benchmarking.

- Documentation.


-------

Please contact me if you find any bugs!  Thanks!


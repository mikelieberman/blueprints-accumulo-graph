Blueprints for Accumulo
=======================

This is an implementation of the Tinkerpop Blueprints API backed by
Accumulo.  The graph is stored in a single table in Accumulo.  This
implementation has support for key/value indexing and some performance
tweaks.  If indexing is enabled, the index is stored in a separate
table.  Everything is serialized using Kryo.  I have not done serious
performance testing, but it seems okay. :)


How to use it
-------------

Create an instance of AccumuloGraphOptions, which allows control over
various features implemented by this adapter using get/set methods.
Then create an instance of AccumuloGraph and pass in the options
object.

Options are as follows:

* Connector info (required): Set zookeepers, instance name, username,
  password.  Essentially the values you need to connect to Accumulo.
  Alternatively, pass in an Accumulo Connector object which represents
  the connection.  If not supplied, mock instance is needed (see
  below).

* Graph table (required): Where to store the graph.

* Index table (optional): Where to store the key/value index.

* Autoflush (optional, default true): Immediately flush changes to
  Accumulo, rather than waiting for performance reasons.  If
  disabled, may cause timing issues (see caveats).

* Return removed property values (optional, default true): The
  removeProperty method specifies that the value of the removed
  property is returned.  This potentially requires another read from
  Accumulo.  If you don't care what is returned, disable this
  to speed things up.

* Use mock instance (optional, default false): If you don't have an
  Accumulo cluster lying around, but still want to use this, you can
  use a "mock" instance of Accumulo which runs in memory and simulates a
  real cluster.


Caveats
-------

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
----

* Read-only usage.  This will enforce only read operations, and would
  allow caching strategies, and avoid timing issues.
* Element/property cache, to increase performance for read-only
  usage.
* Regular-style indexes, in addition to key/value index.
* Tuned querying.
* Bulk loading of graph elements.
* Detailed benchmarking.
* Documentation.


Implementation details
----------------------

The graph is stored in a single table with the following schema.

<table>
<tr><th>Row</th>        <th>CF</th>         <th>CQ</th>         <th>Val</th>        <th>Purpose</th></tr>
<tr><td>MVERTEX</td>    <td>[v id]</td>     <td>-</td>          <td>-</td>          <td>Global vertex list</td></tr>
<tr><td>[v id]</td>     <td>MVERTEX</td>    <td>-</td>          <td>-</td>          <td>Vertex id</td></tr>
<tr><td>[v id]</td>     <td>EOUT</td>       <td>[e id]</td>     <td>[e label]</td>  <td>Vertex out-edge</td></tr>
<tr><td>[v id]</td>     <td>EIN</td>        <td>[e id]</td>     <td>[e label]</td>  <td>Vertex in-edge</td></tr>
<tr><td>MEDGE</td>      <td>[e id]</td>     <td>-</td>          <td>-</td>          <td>Global edge list</td></tr>
<tr><td>[e id]</td>     <td>MEDGE</td>      <td>[e label]</td>  <td>-</td>          <td>Edge id</td></tr>
<tr><td>[e id]</td>     <td>VOUT</td>       <td>[v id]</td>     <td>-</td>          <td>Edge out-vertex</td></tr>
<tr><td>[e id]</td>     <td>VIN</td>        <td>[v id]</td>     <td>-</td>          <td>Edge in-vertex</td></tr>
<tr><td>[v/e id]</td>   <td>PROP</td>       <td>[pname]</td>    <td>[pval]</td>     <td>Element property</td></tr>
</table>

If the index table is enabled, it has the following schema.

<table>
<tr><th>Row</th>        <th>CF</th>         <th>CQ</th>         <th>Val</th>    <th>Purpose</th></tr>
<tr><td>PVLIST</td>     <td>[p name]</td>   <td>-</td>          <td>-</td>      <td>Vertex property list</td></tr>
<tr><td>PELIST</td>     <td>[p name]</td>   <td>-</td>          <td>-</td>      <td>Edge property list</td></tr>
<tr><td>[p name]</td>   <td>[p val]</td>    <td>[v/e id]</td>   <td>-</td>      <td>Property index</td></tr>
</table>


=======

Please contact me if you find any bugs!  Thanks!


LiteRedis
=========

a tiny wrapper for phpiredis for querying a redis (v3+) cluster

```
require('Client.php');
$client = new LiteRedis\Client();
// add a node from the cluster, or add multiple for more redundancy
// this is just used to get the cluster's topology and is not used after
$client->addNode('192.168.1.112',7000);
$client->setClusterTopology($client->getClusterTopology());
$client->set('toto', '123');
echo $client->get('toto');
```
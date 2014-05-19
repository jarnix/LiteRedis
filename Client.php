<?php
namespace LiteRedis;

class Client
{
    
    // all the nodes from the main code
    private $allNodes = array();
    
    // master nodes, for writing
    private $masterNodes = array();
    
    // slave nodes, for reading
    private $slaveNodes = array();
    
    // active connections to each node
    private $connections = array();
    
    // cache for hashes and their corresponding master node
    private $lookupMastersHashTable = array();
    
    // cache for hashes and their corresponding slave node
    private $lookupSlavesHashTable = array();
    
    // commands support and their hashing method
    private $commands = array(
        // -1 : unsupported
        // 0->10 : read
        // 10->20 : write
        // x0 : hashing by first key
        // x1 : hashing by all arguments
        // x2 : hashing by interleaved
        // x3 : hashing by blocking list
        // x4 : hashing by scripting commands
        
        /* commands operating on the key space */
        'EXISTS' => 0,
        'DEL' => 10, // array($this, 'getKeyFromAllArguments')
        'TYPE' => 0,
        'EXPIRE' => 10,
        'EXPIREAT' => 10,
        'PERSIST' => 10,
        'PEXPIRE' => 10,
        'PEXPIREAT' => 10,
        'TTL' => 10,
        'PTTL' => 10,
        'SORT' => - 1,
    
        /* commands operating on string values */
        'APPEND' => 10,
        'DECR' => 10,
        'DECRBY' => 10,
        'GET' => 0,
        'GETBIT' => 0,
        'MGET' => 1,
        'SET' => 10,
        'GETRANGE' => 0,
        'GETSET' => 1,
        'INCR' => 10,
        'INCRBY' => 10,
        'SETBIT' => 10,
        'SETEX' => 10,
        'MSET' => 12,
        'MSETNX' => 12,
        'SETNX' => 10,
        'SETRANGE' => 10,
        'STRLEN' => 0,
        'SUBSTR' => 0,
        'BITCOUNT' => 0,
    
        /* commands operating on lists */
        'LINSERT' => 10,
        'LINDEX' => 0,
        'LLEN' => 0,
        'LPOP' => 10,
        'RPOP' => 10,
        'BLPOP' => 13,
        'BRPOP' => 13,
        'LPUSH' => 10,
        'LPUSHX' => 10,
        'RPUSH' => 10,
        'RPUSHX' => 10,
        'LRANGE' => 10,
        'LREM' => 10,
        'LSET' => 10,
        'LTRIM' => 10,
    
        /* commands operating on sets */
        'SADD' => 10,
        'SCARD' => 0,
        'SISMEMBER' => 0,
        'SMEMBERS' => 0,
        'SSCAN' => 0,
        'SPOP' => 10,
        'SRANDMEMBER' => 0,
        'SREM' => 10,
    
        /* commands operating on sorted sets */
        'ZADD' => 10,
        'ZCARD' => 10,
        'ZCOUNT' => 0,
        'ZINCRBY' => 10,
        'ZRANGE' => 0,
        'ZRANGEBYSCORE' => 0,
        'ZRANK' => 0,
        'ZREM' => 10,
        'ZREMRANGEBYRANK' => 10,
        'ZREMRANGEBYSCORE' => 10,
        'ZREVRANGE' => 0,
        'ZREVRANGEBYSCORE' => 0,
        'ZREVRANK' => 0,
        'ZSCORE' => 0,
        'ZSCAN' => 0,
    
        /* commands operating on hashes */
        'HDEL' => 10,
        'HEXISTS' => 0,
        'HGET' => 0,
        'HGETALL' => 0,
        'HMGET' => 0,
        'HMSET' => 0,
        'HINCRBY' => 10,
        'HINCRBYFLOAT' => 10,
        'HKEYS' => 0,
        'HLEN' => 0,
        'HSET' => 10,
        'HSETNX' => 10,
        'HVALS' => 0,
        'HSCAN' => 0,
    
        /* commands operating on hyperLogLog */
        'PFADD' => 10,
        'PFMERGE' => 10,
        'PFCOUNT' => 10, /* PFCOUNT is not a readonly command because of some server-side caching */
    
        /* scripting */
        'EVAL' => 14, // TODO
        'EVALSHA' => 14 // TODO
        );

    public function __construct()
    {
        if (! function_exists('phpiredis_connect')) {
            throw new \Exception('LiteRedis needs the phpiredis extension to work.');
        }
    }

    public function addNode($ip, $port)
    {
        if (! isset($this->allNodes[$ip . ':' . $port])) {
            $this->allNodes[$ip . ':' . $port] = array(
                'ip' => $ip,
                'port' => $port
            );
        }
    }

    public function getClusterTopology()
    {
        // pick a random node
        $nbTries = 0;
        $client = false;
        
        $nodes = array_values($this->allNodes);
        
        while ($nbTries < count($nodes) && $client === false) {
            $randomNode = $nodes[$nbTries];
            // creates the connection to the random node
            $client = phpiredis_connect($randomNode['ip'], $randomNode['port']);
            $nbTries ++;
        }
        
        if (! $client) {
            throw new \Exception('All nodes of the cluster seem to be down.');
        }
        
        // vérifier que le cluster est sain
        $response = phpiredis_command_bs($client, array(
            'cluster',
            'info'
        ));
        if (strpos($response, 'cluster_state:ok') < 0) {
            throw new \Exception('The cluster is inconsistent');
        }
        
        // get the cluster nodes
        $response = phpiredis_command_bs($client, array(
            'cluster',
            'nodes'
        ));
        // replaces the line concerning this host by a proper syntax
        $response = str_replace(':0 myself,', $randomNode['ip'] . ':' . $randomNode['port'] . ' ', $response);
        
        // list of the master and slave nodes
        $masterNodes = array();
        $slaveNodes = array();
        
        $clusterNodes = explode("\n", $response, - 1);
        $count = count($clusterNodes);
        
        for ($i = 0; $i < $count; $i ++) {
            if (strpos($clusterNodes[$i], 'slave') <= 0) {
                
                $nodeArr = explode(' ', $clusterNodes[$i]);
                
                // we add the master node only if its state is connected
                if ($nodeArr[count($nodeArr) - 1] != 'disconnected') {
                    $slots = explode('-', $nodeArr[8], 2);
                    $host = $nodeArr[1];
                    $hostArr = explode(':', $host);
                    $masterNodes[$nodeArr[0]] = array(
                        'ip' => $hostArr[0],
                        'port' => $hostArr[1],
                        'connected' => ($nodeArr[8] == 'connected'),
                        'min' => $slots[0],
                        'max' => $slots[1]
                    );
                }
            } else {
                $nodeArr = explode(' ', $clusterNodes[$i], 9);
                
                // it the slave is not connected, we will use the master instead without any warning
                if ($nodeArr[7] != 'connected') {
                    $slaveNodes[$nodeArr[3]] = array(
                        'slaveof' => $nodeArr[3]
                    );
                } else {
                    $host = $nodeArr[1];
                    $hostArr = explode(':', $host);
                    $slaveNodes[$nodeArr[0]] = array(
                        'ip' => $hostArr[0],
                        'port' => $hostArr[1],
                        'slaveof' => $nodeArr[3]
                    );
                }
            }
        }
                
        foreach ($slaveNodes as &$slave) {
            // if it's a slave that is not connected, we add the corresponding master
            if (! isset($slave['ip'])) {
                $slave['ip'] = $masterNodes[$slave['slaveof']]['ip'];
                $slave['port'] = $masterNodes[$slave['slaveof']]['port'];
            }
            $slave['min'] = $masterNodes[$slave['slaveof']]['min'];
            $slave['max'] = $masterNodes[$slave['slaveof']]['max'];
        }
        
        // check of the whole cluster, if a slave is missing, then we will add the missing master instead
        foreach ($masterNodes as $masterId=>$masterInfos) {
            // let's try to find the corresponding slave for the same hash slots
            // echo 'master : ' . $masterInfos['min'] . ' -> ' . $masterInfos['max'] . PHP_EOL;
            $slaveFound=false;
            foreach($slaveNodes as $slaveInfos) {
                // echo 'slave : ' . $slaveInfos['min'] . ' -> ' . $slaveInfos['max'] . PHP_EOL;
                if($slaveInfos['min']==$masterInfos['min']) {
                    $slaveFound=true;
                    break;
                }
            }
            if(!$slaveFound) {
                $slaveNodes[$masterId]=$masterInfos;
            }
        }
                
        $this->masterNodes = $masterNodes;
        $this->slaveNodes = $slaveNodes;
    }

    public function __call($method, $args = array())
    {
        if (! count($this->masterNodes) && ! count($this->slaveNodes)) {
            $this->getClusterTopology();
        }
        
        $command = strtoupper($method);
        
        if (isset($this->commands[$command])) {
            
            $hashingAlgo = $this->commands[$command];
            
            $isCommandForSlave = true;
            
            // if it's a write command, we will query a master
            if ($hashingAlgo >= 10) {
                $isCommandForSlave = false;
                $hashingAlgo = $hashingAlgo - 10;
            }
            
            if (count($args)) {
                
                $keyUsedForHash = null;
                
                switch ($hashingAlgo) {
                    case 0:
                        $keyUsedForHash = $args[0];
                        break;
                    case 1:
                        if (count($args) == 1) {
                            $keyUsedForHash = $args[0];
                        }
                        break;
                    case 2:
                        if (count($args) == 2) {
                            $keyUsedForHash = $args[0];
                        }
                        break;
                    case 3:
                        if (count($args) == 2) {
                            $keyUsedForHash = $args[0];
                        }
                        break;
                    case 4:
                        throw new \Exception('Scripting is not yet supported by this class');
                        break;
                }
                
                if ($keyUsedForHash != null) {
                    $hash = $this->hash($keyUsedForHash) % 16384;
                    
                    $selectedNodes = ($isCommandForSlave ? $this->slaveNodes : $this->masterNodes);
                    
                    $chosenNodeId = null;
                    
                    if ($isCommandForSlave) {
                        if (isset($this->lookupSlavesHashTable[$hash])) {
                            $chosenNodeId = $this->lookupSlavesHashTable[$hash];
                        }
                    } else {
                        if (isset($this->lookupMastersHashTable[$hash])) {
                            $chosenNodeId = $this->lookupMastersHashTable[$hash];
                        }
                    }
                    
                    // if we do not have the matching node in "cache", let's find it
                    if ($chosenNodeId == null) {
                        foreach ($selectedNodes as $potentialNodeId => $potentialNode) {
                            if ($hash >= $potentialNode['min'] && $hash <= $potentialNode['max']) {
                                $chosenNodeId = $potentialNodeId;
                                break;
                            }
                        }
                    }
                    
                    if ($chosenNodeId == null) {
                        
                        echo $command . ' ' . implode(' ', $args) . PHP_EOL;
                        
                        if ($isCommandForSlave) {
                            print_r($this->slaveNodes);
                        }
                        else {
                            print_r($this->masterNodes);
                        }
                        
                        
                        throw new \Exception('Cannot find a node for this slot: ' . $hash);
                    } else {
                        if ($isCommandForSlave) {
                            $this->lookupSlavesHashTable[$hash] = $chosenNodeId;
                        } else {
                            $this->lookupMastersHashTable[$hash] = $chosenNodeId;
                        }
                        
                        
                        $connection = $this->connect($chosenNodeId, $selectedNodes[$chosenNodeId]);
                        if ($isCommandForSlave) {
                            $fullCommand = array(
                                'READONLY',
                                $command . ' ' . implode(' ', $args)
                            );
                            // echo $command . ' ' . implode(' ', $args) . PHP_EOL;
                            $output = phpiredis_multi_command($connection, $fullCommand);
                            return $output;
                        } else {
                            foreach ($args as &$arg) {
                                $arg = strval($arg);
                            }
                            $output = phpiredis_command_bs($connection, array_merge(array(
                                $command
                            ), (array) ($args)));
                            // echo $command . ' ' . implode(' ', $args) . PHP_EOL;
                            return $output;
                        }
                    }
                }
            }
        }
    }

    private function connect($nodeId, $node)
    {
        
        if (! isset($this->connections[$nodeId])) {
            $this->connections[$nodeId] = phpiredis_connect($node['ip'], $node['port']);
        }
        if ($this->connections[$nodeId] == false) {
            throw new \Exception('The node ' . $node['ip'] . ':' . $node['port'] . ' seems to be down');
        }
        return $this->connections[$nodeId];
    }

    private function hash($value)
    {
        // CRC-CCITT-16 algorithm
        $crc = 0;
        $CCITT_16 = self::$CCITT_16;
        $strlen = strlen($value);
        
        for ($i = 0; $i < $strlen; $i ++) {
            $crc = (($crc << 8) ^ $CCITT_16[($crc >> 8) ^ ord($value[$i])]) & 0xFFFF;
        }
        
        return $crc;
    }

    private static $CCITT_16 = array(
        0x0000,
        0x1021,
        0x2042,
        0x3063,
        0x4084,
        0x50A5,
        0x60C6,
        0x70E7,
        0x8108,
        0x9129,
        0xA14A,
        0xB16B,
        0xC18C,
        0xD1AD,
        0xE1CE,
        0xF1EF,
        0x1231,
        0x0210,
        0x3273,
        0x2252,
        0x52B5,
        0x4294,
        0x72F7,
        0x62D6,
        0x9339,
        0x8318,
        0xB37B,
        0xA35A,
        0xD3BD,
        0xC39C,
        0xF3FF,
        0xE3DE,
        0x2462,
        0x3443,
        0x0420,
        0x1401,
        0x64E6,
        0x74C7,
        0x44A4,
        0x5485,
        0xA56A,
        0xB54B,
        0x8528,
        0x9509,
        0xE5EE,
        0xF5CF,
        0xC5AC,
        0xD58D,
        0x3653,
        0x2672,
        0x1611,
        0x0630,
        0x76D7,
        0x66F6,
        0x5695,
        0x46B4,
        0xB75B,
        0xA77A,
        0x9719,
        0x8738,
        0xF7DF,
        0xE7FE,
        0xD79D,
        0xC7BC,
        0x48C4,
        0x58E5,
        0x6886,
        0x78A7,
        0x0840,
        0x1861,
        0x2802,
        0x3823,
        0xC9CC,
        0xD9ED,
        0xE98E,
        0xF9AF,
        0x8948,
        0x9969,
        0xA90A,
        0xB92B,
        0x5AF5,
        0x4AD4,
        0x7AB7,
        0x6A96,
        0x1A71,
        0x0A50,
        0x3A33,
        0x2A12,
        0xDBFD,
        0xCBDC,
        0xFBBF,
        0xEB9E,
        0x9B79,
        0x8B58,
        0xBB3B,
        0xAB1A,
        0x6CA6,
        0x7C87,
        0x4CE4,
        0x5CC5,
        0x2C22,
        0x3C03,
        0x0C60,
        0x1C41,
        0xEDAE,
        0xFD8F,
        0xCDEC,
        0xDDCD,
        0xAD2A,
        0xBD0B,
        0x8D68,
        0x9D49,
        0x7E97,
        0x6EB6,
        0x5ED5,
        0x4EF4,
        0x3E13,
        0x2E32,
        0x1E51,
        0x0E70,
        0xFF9F,
        0xEFBE,
        0xDFDD,
        0xCFFC,
        0xBF1B,
        0xAF3A,
        0x9F59,
        0x8F78,
        0x9188,
        0x81A9,
        0xB1CA,
        0xA1EB,
        0xD10C,
        0xC12D,
        0xF14E,
        0xE16F,
        0x1080,
        0x00A1,
        0x30C2,
        0x20E3,
        0x5004,
        0x4025,
        0x7046,
        0x6067,
        0x83B9,
        0x9398,
        0xA3FB,
        0xB3DA,
        0xC33D,
        0xD31C,
        0xE37F,
        0xF35E,
        0x02B1,
        0x1290,
        0x22F3,
        0x32D2,
        0x4235,
        0x5214,
        0x6277,
        0x7256,
        0xB5EA,
        0xA5CB,
        0x95A8,
        0x8589,
        0xF56E,
        0xE54F,
        0xD52C,
        0xC50D,
        0x34E2,
        0x24C3,
        0x14A0,
        0x0481,
        0x7466,
        0x6447,
        0x5424,
        0x4405,
        0xA7DB,
        0xB7FA,
        0x8799,
        0x97B8,
        0xE75F,
        0xF77E,
        0xC71D,
        0xD73C,
        0x26D3,
        0x36F2,
        0x0691,
        0x16B0,
        0x6657,
        0x7676,
        0x4615,
        0x5634,
        0xD94C,
        0xC96D,
        0xF90E,
        0xE92F,
        0x99C8,
        0x89E9,
        0xB98A,
        0xA9AB,
        0x5844,
        0x4865,
        0x7806,
        0x6827,
        0x18C0,
        0x08E1,
        0x3882,
        0x28A3,
        0xCB7D,
        0xDB5C,
        0xEB3F,
        0xFB1E,
        0x8BF9,
        0x9BD8,
        0xABBB,
        0xBB9A,
        0x4A75,
        0x5A54,
        0x6A37,
        0x7A16,
        0x0AF1,
        0x1AD0,
        0x2AB3,
        0x3A92,
        0xFD2E,
        0xED0F,
        0xDD6C,
        0xCD4D,
        0xBDAA,
        0xAD8B,
        0x9DE8,
        0x8DC9,
        0x7C26,
        0x6C07,
        0x5C64,
        0x4C45,
        0x3CA2,
        0x2C83,
        0x1CE0,
        0x0CC1,
        0xEF1F,
        0xFF3E,
        0xCF5D,
        0xDF7C,
        0xAF9B,
        0xBFBA,
        0x8FD9,
        0x9FF8,
        0x6E17,
        0x7E36,
        0x4E55,
        0x5E74,
        0x2E93,
        0x3EB2,
        0x0ED1,
        0x1EF0
    );
}
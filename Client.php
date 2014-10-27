<?php
namespace LiteRedis;

class Client
{
    
    // the nodes given by the application
    private $addedNodes = array();
    
    // all the nodes by id
    private $allNodes = array();
    
    // slots
    private $slots = array();
    
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
        'EVALSHA' => 14
    ); // TODO

    public function __construct()
    {
        if (! function_exists('phpiredis_connect')) {
            throw new \Exception('LiteRedis needs the phpiredis extension to work.');
        }
    }

    public function addNode($ip, $port)
    {
        if (! isset($this->addedNodes[$ip . ':' . $port])) {
            $this->addedNodes[$ip . ':' . $port] = array(
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
        
        $nodes = array_values($this->addedNodes);
        
        while ($nbTries < count($nodes) && $client === false) {
            $randomNode = $nodes[$nbTries];
            // creates the connection to the random node
            $client = phpiredis_connect($randomNode['ip'], $randomNode['port']);
            $nbTries ++;
        }
        
        if (! $client) {
            throw new \Exception('All nodes of the cluster seem to be down.');
        }
        
        // verify that the cluster is consistent
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
        
        /*
         * $response = "4c018a7d3da91858afbb9eb9b376ca81a5e63c11 192.168.1.208:7100 master - 0 1414154054475 41 connected
         * 01939672432f215c58ffd07e60a627560bff866a 192.168.1.105:7000 master - 0 1414154055477 40 connected 4596-8191 4596-8191 4596-8191
         * 42d4c6c023a5584c387902f39a4fdec7c2dc38c0 192.168.1.96:7000 master - 0 1414154056480 36 connected 8692-12287
         * 6cae0b7c0a1ae753553d5949e58099c7e344e2ef 192.168.1.105:7100 slave f5c10b16cbc28bf86c2092ad74b0071fbb1d3a9e 0 1414154055978 6 connected
         * 0e06e0c85d7830a8a40e1d158fc5503d18e2f188 192.168.1.88:7100 slave 5dbf2e6a587982f99528084b059950bbac9d67db 0 1414154055477 42 connected
         * 1f695374f0d0f6721df7b54484d2843cdbd895e6 192.168.1.88:7000 slave 4c018a7d3da91858afbb9eb9b376ca81a5e63c11 0 1414154054976 41 connected
         * f5c10b16cbc28bf86c2092ad74b0071fbb1d3a9e 192.168.1.112:7000 myself,master - 0 0 1 connected 500-4095
         * 5dbf2e6a587982f99528084b059950bbac9d67db 192.168.1.208:7000 master - 0 1414154055978 42 connected 0-499 4096-4595 8192-8691 12288-12787
         * f6afa8eb6a5fe4ef7d9dec007c847acef63a8ef2 192.168.1.178:7000 master - 0 1414154055978 34 connected 12788-16383
         * 0943d94c3053003911423a4fb872dcacd853fdc7 192.168.1.112:7100 slave 01939672432f215c58ffd07e60a627560bff866a 0 1414154054475 40 connected
         * eb76c3e4f3646182b8f0a4e83a8a459af8f17987 192.168.1.96:7100 slave f6afa8eb6a5fe4ef7d9dec007c847acef63a8ef2 0 1414154054475 34 connected
         * 88ef18626abf4a2911e8edde8466f2911314ea45 192.168.1.178:7100 slave 42d4c6c023a5584c387902f39a4fdec7c2dc38c0 0 1414154054976 36 connected";
         */
        
        // list of the slots
        $slots = array();
        
        // list of the master and slave nodes
        $masterNodes = array();
        $slaveNodes = array();
        
        $alreadySeenSlots = array();
        
        $clusterNodes = explode("\n", $response);
        $count = count($clusterNodes);
        
        for ($i = 0; $i < $count; $i ++) {
            
            if (strpos($clusterNodes[$i], 'slave') === false) {
                
                $nodeArr = explode(' ', $clusterNodes[$i]);
                
                // we add the master node only if its state is connected
                if (strpos($clusterNodes[$i], ' connected') !== false) {
                    
                    $host = $nodeArr[1];
                    $hostArr = explode(':', $host);
                    
                    $masterNodes[$nodeArr[0]] = array(
                        'ip' => $hostArr[0],
                        'port' => $hostArr[1]
                    );
                    
                    if (preg_match_all('/(\d+)\-(\d+)/', $clusterNodes[$i], $matches, PREG_SET_ORDER)) {
                        foreach ($matches as $match) {
                            
                            $min = $match[1];
                            $max = $match[2];
                            
                            if (! in_array($min . '-' . $max, $alreadySeenSlots)) {
                                
                                $slots[] = array(
                                    'min' => $min,
                                    'max' => $max,
                                    'master' => array(
                                        'ip' => $hostArr[0],
                                        'port' => $hostArr[1],
                                        'id' => $nodeArr[0]
                                    )
                                );
                                
                                $alreadySeenSlots[] = $min . '-' . $max;
                            }
                        }
                    }
                }
            } else {
                
                $nodeArr = explode(' ', $clusterNodes[$i]);
                
                $host = $nodeArr[1];
                $hostArr = explode(':', $host);
                
                if (preg_match('/slave ([a-f0-9]+)/', $clusterNodes[$i], $matches)) {
                    $slaveOf = $matches[1];
                    
                    // it the slave is not connected, we will use the master instead without any warning
                    if (strpos($clusterNodes[$i], ' connected') === false) {
                        $slaveNodes[$nodeArr[0]] = array(
                            'slaveof' => $slaveOf
                        );
                    } else {
                        $slaveNodes[$nodeArr[0]] = array(
                            'ip' => $hostArr[0],
                            'port' => $hostArr[1],
                            'slaveof' => $slaveOf
                        );
                    }
                }
            }
        }
        
        foreach ($slaveNodes as $nodeId => $slaveInfos) {
            
            // if it's a slave that is not connected, we add the corresponding master
            if (! isset($slaveInfos['ip'])) {
                $slaveInfos['ip'] = $masterNodes[$slaveInfos['slaveof']]['ip'];
                $slaveInfos['port'] = $masterNodes[$slaveInfos['slaveof']]['port'];
            }
            foreach ($slots as &$slot) {
                if ($slot['master']['id'] == $slaveInfos['slaveof']) {
                    $slot['slave'] = array(
                        'id' => $nodeId,
                        'ip' => $slaveInfos['ip'],
                        'port' => $slaveInfos['port']
                    );
                }
            }
            reset($slots);
            unset($slot);
        }
        
        return $slots;
    }

    public function setClusterTopology($slots)
    {
        $this->slots = $slots;
        $nodeTypes = array(
            'master',
            'slave'
        );
        foreach ($this->slots as $slot) {
            foreach ($nodeTypes as $nodeType)
                if (! isset($this->allNodes[$slot[$nodeType]['id']])) {
                    $this->allNodes[$slot[$nodeType]['id']] = array(
                        'ip' => $slot[$nodeType]['ip'],
                        'port' => $slot[$nodeType]['port']
                    );
                }
        }
                
    }

    public function __call($method, $args = array())
    {
        if (! count($this->slots)) {
            throw new \Exception('You need to setup the cluster topology');
        } else {
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
                        
                        $selectedNodeType = ($isCommandForSlave ? 'slave' : 'master');
                        
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
                            foreach ($this->slots as $slot) {
                                if ($hash >= $slot['min'] && $hash <= $slot['max']) {
                                    $chosenNodeId = $slot[$selectedNodeType]['id'];
                                    break;
                                }
                            }
                        }
                        
                        if ($chosenNodeId == null) {
                            throw new \Exception('Cannot find a node for this slot: ' . $hash);
                        } else {
                            if ($isCommandForSlave) {
                                $this->lookupSlavesHashTable[$hash] = $chosenNodeId;
                            } else {
                                $this->lookupMastersHashTable[$hash] = $chosenNodeId;
                            }
                            $connection = $this->connect($chosenNodeId, $this->allNodes[$chosenNodeId]['ip'], $this->allNodes[$chosenNodeId]['port']);
                            if ($isCommandForSlave) {
                                $fullCommand = array(
                                    'READONLY',
                                    $command . ' ' . implode(' ', $args)
                                );
                                $output = phpiredis_multi_command($connection, $fullCommand);
                                return $output[1];
                            } else {
                                foreach ($args as &$arg) {
                                    $arg = strval($arg);
                                }
                                $output = phpiredis_command_bs($connection, array_merge(array(
                                    $command
                                ), (array) ($args)));
                                return $output;
                            }
                        }
                    }
                }
            }
        }
    }

    private function connect($nodeId, $ip, $port)
    {
        if (! isset($this->connections[$nodeId])) {
            $this->connections[$nodeId] = phpiredis_connect($ip, $port);
        }
        if ($this->connections[$nodeId] == false) {
            throw new \Exception('The node ' . $ip . ':' . $port . ' seems to be down');
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
<?php

function errHandle($errNo, $errStr, $errFile, $errLine) {
    $msg = "$errStr in $errFile on line $errLine";
    if ($errNo == E_NOTICE || $errNo == E_WARNING) {
        throw new ErrorException($msg, $errNo);
    } else {
        echo $msg;
    }
}

set_error_handler('errHandle');

require('Client.php');

ini_set('break_on_errors', true);

$client = new LiteRedis\Client();

$client->addNode('192.168.1.112',7000);

$client->setClusterTopology($client->getClusterTopology());

$client->set('toto', '123');

echo $client->get('toto');
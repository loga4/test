<?php

require 'vendor/autoload.php';
Predis\Autoloader::register();

$redispass = 'c@o4NCd_TLkFasH4zWd*voQb';

// $sentinels = ['tcp://rc1b-hg0uh0xqbtg8arlp.mdb.yandexcloud.net:26379'];
// $options = [
//     'replication' => 'sentinel',
//     'service' => 'redis578',
//     'parameters' => [
//         'password' => $redispass
//     ]
// ];
// $client = new Predis\Client($sentinels, $options);
$client = new Predis\Client();
$client->flushall();

$logFile = "gen.log";
file_put_contents('gen.log','');

$queueClients = "bothelp_clients";
$maxClients = 1000;
$maxEvents = 10000;
$maxEventsForClient = 5;

//generate events
for ($i=0; $i<$maxEvents; $i++)  {
    $cid = rand(1, $maxClients);
    
    //generate pack events for one client
    $events = range(1, $maxEventsForClient);
    shuffle($events);

    foreach($events as $eid) {
        //some data
        $type = md5(time() . $cid . $eid);

        $event = [
            'i' => $i,
            'cid' => $cid,
            'eid' => $eid,
            'type' => $type,
        ];
        $event = json_encode($event);
    
        $queue = "bothelp_$cid";
    
        $client->transaction()
            ->sadd($queueClients, $cid)
            ->rpush($queue, $event)
            ->execute();

        file_put_contents($logFile, "$queue\t$event\n", FILE_APPEND);
    }
}
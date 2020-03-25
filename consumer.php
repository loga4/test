<?php
require 'vendor/autoload.php';

class Consumer {
    protected $client;
    protected $http;
    protected $queue = "bothelp_clients";

    public function __construct() {
        $this->client = new Predis\Client();
        $this->http = new \GuzzleHttp\Client();
    }

    public function process() {
        while(true) {
            try {
                $clientId = $this->client->spop($this->queue);
                if (empty($clientId)) { sleep(5); continue; }

                echo $clientId ."\n";
                $this->http->get("http://bothelp.lo/worker.php?cid=$clientId");
                
            // возвращаем клиента обратно в set
            } catch (GuzzleHttp\Exception\ConnectException $e) {
                echo $e->getMessage();
                $this->client->sadd($this->queue, $clientId);
                sleep(5);

            } catch(\Throwable $e) {
                echo $e->getMessage();
            }
        }
    }
}

$consumer = new Consumer();
$consumer->process();
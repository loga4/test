<?php 

require 'vendor/autoload.php';

class BadNetworkException extends Exception {}
class BadJsonException extends Exception {}

class Worker {

    const RETRY_COUNT = 1;
    const QUEUE_BLOCK_TTL = 10;

    protected $client;
    protected $queue;

    public function __construct() {
        $this->client = new Predis\Client();
    }

    public function run(int $clientId) {
        $this->sendFastcgiHeaders();
        $this->process($clientId);
    }

    protected function sendFastcgiHeaders() {
        ignore_user_abort(true);
        fastcgi_finish_request();
    }

    public function process(int $clientId) {
        $this->queue = "bothelp_{$clientId}";
        $this->queueBlock = "{$this->queue}:block";

        //проверяем если блокировка клиента
        $block = $this->client->get($this->queueBlock);
        if ($block) { return false; }

        while(true) {
            try {
                //ставим блокировку и ttl - если воркер упадет, чтобы блокировка снялась средствами redis
                $this->client
                    ->setex($this->queueBlock, self::QUEUE_BLOCK_TTL, 1);

                $event = $this->client->lpop($this->queue);
                if (empty($event)) {
                    break;
                }
        
                $data = json_decode($event);
        
                //важная работа
                sleep(1);
                
                //эмулируем проблему с сетью, к примеру не удалось достать доп данные по клиенту
                if ($clientId > 500 && $data->eid % 2 === 0 && (empty($data->retry) || $data->retry < self::RETRY_COUNT )) {
                    throw new BadNetworkException('bad network');
                }
        
                $message = date('c') . "\t$event\n";
                $this->log($message);

            //очень важно ловить каждое исключение - тогда работа воркера будет предсказуема
            } catch (BadNetworkException $e) {
                $message = date('c') . "\t$event - retry event {$e->getMessage()}\n";
                $this->log($message);
        
                $this->retryEvent($event);

            // ну или не валидный json - здесь пробуем вернуть, но возможно надо кинуть исключение выше
            } catch (BadJsonException $e) {
                $message = date('c') . "\t$event - retry event {$e->getMessage()}\n";
                $this->log($message);
        
                $this->retryEvent($event);

            
            //ну или коннект к редису не валидный - зануляем коннект и выходим
            } catch (Predis\Network\ConnectionException $e) {
                $this->client = null;
                break;

            } catch (\Throwable $e) {
                //add logic for unknown exception
                //threow new CantProcessException();

            } finally {
                //send some metric
            }
        }

        //если воркер отработал - снимаем блок и умираем
        if ($this->client) {
            $this->client->del($this->queueBlock);
        }
    }

    protected function retryEvent($event) {
        $data = json_decode($event);
        $data->retry = isset($data->retry) ? $data->retry+1 : 0;
        $event = json_encode($data);

        $this->client->lpush($this->queue, $event);
    }

    protected function log($message) {
        file_put_contents('log/' . $this->queue, $message, FILE_APPEND);
    }
}

$cid = $_GET['cid'] ?? null;

if (empty($cid)) { die("option -cid required\n"); }

$worker = new Worker();
$worker->run($cid);

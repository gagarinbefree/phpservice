<?php
function Start1c($dbParam) {
    try {
        Soap($dbParam);
    }
    catch (Exception $e) {
        throw $e;
    }
}

function Soap($dbParam) {
    try {
        $res = SelectQueue($dbParam);
        $row = ibase_fetch_object($res);

        if ($row != false) {
            $Log1cId = null;
            $Log1cId = InsertLogs1c($dbParam);

            if ($Log1cId !== null) {
                $conf = null;
                try {
                    // получим конфигурацию
                    $conf = SelectConf($dbParam);
                    if ($conf != null) {
                        try {
                            $object = new SimpleXMLElement('<?xml version="1.0" encoding="UTF-8" standalone="yes"?><elements/>');

                            $ii = 0;
                            while ($row != false) {
                                // есть id заказа? (без id старые заказы, просто пропустим)
                                if ($row->PROMORDERID != null) {
                                    $XMLRow = $object->addChild('row');
                                    $XMLRow['datecreated']=date('Y-m-d H:i:s', strtotime($row->DATETIME));
                                    if ($row->SENDER !== '') {
                                        $XMLRow['warehouse_sender'] = iconv('windows-1251', 'utf-8', $row->SENDER);
                                    }
                                    if($row->RECIPIENT !== '') {
                                        $XMLRow['warehouse_recipient'] = iconv('windows-1251', 'utf-8', $row->RECIPIENT);
                                    }
                                    $XMLRow['orderid'] = $row->PROMORDERID;
                                    $XMLRow['zayavka'] = iconv('windows-1251', 'utf-8', $row->ORDERNO);
                                    $XMLRow['marking'] = $row->CODE1C;
                                    $XMLRow['calc'] = $row->CALC;
                                    $XMLRow['queueid'] = $row->QUEUEID;

                                    // запись в промежуточной таблице для связи один ко многим таблиц QUEUE и LOGS1C
                                    InsertQueueLogs1c($dbParam, $row->QUEUEID, $Log1cId);
                                }
                                $row = ibase_fetch_object($res);
                                $ii++;
                            }
                            if ($ii > 0) {
                                $params = array(
                                    "param1" => "queue",
                                    "param2" => $object->asXML()
                                );

                                $client = new SoapClient($conf->WSURL, array("login"=>$conf->WSUSERNAME, "password"=>$conf->WSPASSWORD));
                                $res = $client->djinn($params);

                                $xml = new SimpleXMLElement($res->param2);
                                foreach ($xml as $el)
                                {
                                    UpdateQueue($dbParam, $el['queueid'], 1, $el['failcode']);
                                }

                                // ошибок нет
                                UpdateLogs1c($dbParam, $Log1cId, null);
                            }
                            else {
                                try {
                                    UpdateLogs1c($dbParam, $Log1cId, 'Не удалось создать XML');
                                }
                                catch (Exception $e) {
                                    throw $e;
                                }
                            }
                        }
                        catch (Exception $e) {
                            try {
                                UpdateLogs1c($dbParam, $Log1cId, $e->getMessage());
                            }
                            catch (Exception $e) {
                                throw $e;
                            }
                        }
                    }
                    else {
                        try {
                            UpdateLogs1c($dbParam, $Log1cId, 'Не удалось получить запись из CONF');
                        }
                        catch (Exception $e) {
                            throw $e;
                        }
                    }
                }
                catch (Exception $e) {
                    try {
                        UpdateLogs1c($dbParam, $Log1cId, $e->getMessage());
                    }
                    catch (Exception $e) {
                        throw $e;
                    }
                }
            }
            else {
                throw new Exception('Запись в LOGS1C не создана');
            }
        }
    }
    catch (Exception $e) {
        throw $e;
    }
}

function UpdateQueue($dbParam, $queueIdParam, $processedParam, $resultParam) {
    try {
        $query = 'update queue set processed = ?, result = ?  where queueid = ?';
        ibase_query($dbParam, $query, $processedParam, $resultParam, $queueIdParam);
    }
    catch (Exception $e) {
        throw $e;
    }
}

function SelectConf($dbParam) {
    try {
        $query = 'select * from conf';
        $res = ibase_query($dbParam, $query);

        return ibase_fetch_object($res);
    }
    catch (Exception $e) {
        throw $e;
    }
}

function InsertLogs1c($dbParam) {
    try {
        $query = 'INSERT INTO logs1c (startdate) values (CURRENT_TIMESTAMP) RETURNING log1cid';
        $res = ibase_query($dbParam,  $query);
        $ret = ibase_fetch_row($res);

        return $ret[0];
    }
    catch (Exception $e) {
        throw $e;
    }
}

function UpdateLogs1c($dbParam, $log1cIdParam, $errorParam) {
    try {
        $query = "update logs1c set enddate = CURRENT_TIMESTAMP, error = ? where log1cid = ?";
        $res = ibase_query($dbParam, $query, $errorParam, $log1cIdParam) or die (ibase_errmsg());
    }
    catch (Exception $e) {
        throw $e;
    }
}

function InsertQueueLogs1c($dbParam, $queueIdParam, $log1cIdParam) {
    try {
        $query = 'INSERT INTO queuelogs1c (queueid, log1cid) values (? , ?)';
        ibase_query($dbParam,  $query, $queueIdParam, $log1cIdParam);
    }
    catch (Exception $e) {
        throw $e;
    }
}

function SelectQueue($dbParam)
{
    try {
        $query = 'SELECT queueid, orderno, code1c, calc, promorderid, sender, recipient, datetime
            FROM queue WHERE processed = 0 ORDER BY datetime';

        return ibase_query($dbParam, $query);
    }
    catch (Exception $e) {
        throw $e;
    }
}
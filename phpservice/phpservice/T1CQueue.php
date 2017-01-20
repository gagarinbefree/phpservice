<?php
class T1CQueueElement{
    public $warehouseSender;
    public $warehouseRecipient;
    public $orderId;
    public $orderNo;
    public $marking;
    public $cnt;
    public $dateCreated;

    function __construct($warehouseSender, $warehouseRecipient,$orderid,$orderNo,$marking,$cnt,$dateCreated){
        $this->warehouseSender = $warehouseSender;
        $this->warehouseRecipient = $warehouseRecipient;
        $this->orderId = $orderid;
        $this->orderNo = $orderNo;
        $this->marking = $marking;
        $this->cnt = $cnt;
        $this->dateCreated = $dateCreated;
    }
}

abstract class T1CQueue{
    private $_serviceURL;
    private $_serviceUser;
    private $_servicePassword;

    function __construct($serviceURL, $serviceUser, $servicePassword){
        $this->_serviceURL = $serviceURL;
        $this->_serviceUser = $serviceUser;
        $this->_servicePassword = $servicePassword;
    }

    abstract protected function Insert1CSyncLog($db);
    abstract protected function Fill1CSyncLog($db, $id);
    abstract protected function Write1CSyncLogError($db, $id, $errText);
    abstract protected function Update1CSyncLogProcessed($db,$id,$warnings);
    abstract protected function Update1CQueueElementOK_Prepare($db);
    abstract protected function Update1CQueueElementFailed_Prepare($db);
    abstract protected function Update1CQueueElementOK($db, $preparedQuery, $orderid, $markind);
    abstract protected function Update1CQueueElementFailed($db, $preparedQuery, $orderid, $marking, $errText);

    /**
     * @param $db resource
     * @param $id Number
     * @return array
     */
    abstract protected function GetSyncElementsForXML($db, $id);

    protected function ansiToUtf($val){
        return iconv("windows-1251", "utf-8", $val);
    }
    protected function utfToAnsi($val){
        return iconv("utf-8", "windows-1251", $val);
    }

    private function GenerateSyncXML($db,$id){
        $arr = $this->GetSyncElementsForXML($db, $id);
        $arrCnt = count($arr);
        if ($arrCnt > 0){
            $XML = new SimpleXMLElement('<?xml version="1.0" encoding="UTF-8" standalone="yes"?><elements/>');
            for ($i = 0; $i < $arrCnt; $i++){
                /** @var $e T1CQueueElement */
                $e = $arr[$i];
                $XMLRow = $XML->addChild('Row');
                $XMLRow['warehouse_sender'] = $this->ansiToUtf($e->warehouseSender);
                if(empty($e->warehouseRecipient))
                    $XMLRow['warehouse_recipient'] = $this->ansiToUtf($e->warehouseRecipient);
                $XMLRow['orderid'] = $this->ansiToUtf($e->orderId);
                $XMLRow['zayavka'] = $this->ansiToUtf($e->orderNo);
                $XMLRow['marking'] = $this->ansiToUtf($e->marking);
                $XMLRow['calc'] = $this->ansiToUtf($e->cnt);
                $XMLRow['dt'] = $this->ansiToUtf($e->dateCreated);
            };
            return $XML;
        }else
            return null;
    }

    /**
     * @param $XML SimpleXMLElement
     * @return SimpleXMLElement
     * @throws Exception
     */
    private function ServiceCall($XML){
        $sXML = $XML->asXML();
        $params = array(
            "param1" => "queue",
            "param2" => $sXML
        );
        $user = array("login"=>$this->_serviceUser,
            "password"=>$this->_servicePassword
        );
        $client = new SoapClient($this->_serviceURL,$user);

        $client->djinn($params); // вызов Soap метода
        $sXML = $params['param2']; // результирующий XML
        if (empty($sXML))
            throw new Exception("Вернулся пустой XML");
        $NewXml = new SimpleXMLElement($sXML);
        return $NewXml;
    }

    private function updateResults($db,$Xml){
        // подготовка запросов для множественного выполнения
        $qAccepted = $this->Update1CQueueElementOK_Prepare($db);
        $qFailed = $this->Update1CQueueElementFailed_Prepare($db);

        $CountRow = count($Xml->Row);
        for ($i = 0; $i < $CountRow; $i++){
            $row = $Xml->Row[$i];
            $orderid = $this->utfToAnsi($row['orderid']);
            $MARKING = $this->utfToAnsi($row['marking']);
            $oshibka = $this->utfToAnsi($row['failcode']);

            if (!empty($oshibka)){
                $this->Update1CQueueElementFailed($db, $qFailed, $orderid, $MARKING, $oshibka);
            }else{
                $this->Update1CQueueElementOK($db, $qAccepted, $orderid, $MARKING);
            }
        }
    }

    private function iniWarningsOn(){
        $prev = ini_get('display_errors');
        ini_set('display_errors', '1');
        return $prev;
    }

    private function iniWarningsOff($prev){
        if ($prev != '1')
            ini_set('display_errors', $prev);
    }

    /**
     *@throws Exception
     */
    public function Process_1C_Queue($db) {
        ob_start();
        ob_clean();
        $prev_iniWarnings = $this->iniWarningsOn();
        $Service1CId =$this->Insert1CSyncLog($db);
        try{
            $this->Fill1CSyncLog($db,$Service1CId);
            $XML = $this->GenerateSyncXML($db,$Service1CId);
            if (!empty($XML)){
                $NewXml = $this->ServiceCall($XML);
                $this->updateResults($db,$NewXml);
            }
            $warnings = ob_get_contents();
            $this->Update1CSyncLogProcessed($db,$Service1CId, $warnings);
        }catch(Exception $e){
            $this->Write1CSyncLogError($db,$Service1CId,$e->getMessage());
        };
        ob_end_clean();
        $this->iniWarningsOff($prev_iniWarnings);
    }
};

class T1CSkladQueue extends T1CQueue{
    protected function Insert1CSyncLog($db)
    {
        $id = ibase_gen_id('gen_logs1c_id', 1, $db);
        if (!($id > 0)) {
            throw new Exception(ibase_errmsg());
        }

        $sql = 'INSERT INTO logs1c (log1cid, startdate) VALUES (?, current_timestamp)';
        $q = ibase_prepare($db, $sql);
        if (!$q) {
            throw new Exception(ibase_errmsg());
        }

        if (!ibase_execute($q, $id)) {
            throw new Exception(ibase_errmsg());
        }

        return $id;
    }

    protected function Fill1CSyncLog($db, $id)
    {
        $sql='INSERT INTO queuelogs1c (log1cid, queueid) VALUES (?,?);';
        $q2 = ibase_prepare($db, $sql);
        if (!$q2) {
            throw new Exception(ibase_errmsg());
        }

        $sql = "select w.queueid
				from queue w
				left join orders o on o.promorderid = w.promorderid
				where w.processed = 0
				and w.result is null";
        $res = ibase_query($db, $sql);
        if (!$res) {
            throw new Exception(ibase_errmsg());
        }

        while (($row = ibase_fetch_object($res))) {
            if (!ibase_execute($q2, $id, $row->QUEUEID)) {
                throw new Exception(ibase_errmsg());
            }
        };
    }

    /**
     * @param $db
     * @param $id
     * @throws Exception
     * @return array
     */
    protected function GetSyncElementsForXML($db, $id)
    {
        $arr = array();
        $sql = "select w.promorderid, o.orderno, w.sender, w.recipient, w.code1c, w.datetime, w.calc
				from queuelogs1c q
				left join queue w on w.queueid = q.queueid
				left join orders o on o.promorderid = w.promorderid
				where q.log1cid = ?";
        $q = ibase_prepare($db, $sql);
        if (!$q)
            throw new Exception(ibase_errmsg());
        $res = ibase_execute($q, $id);
        if (!$res)
            throw new Exception(ibase_errmsg());
        while (($row = ibase_fetch_assoc($res))){
            $e = new T1CQueueElement($row['SENDER'], $row['RECIPIENT'], $row['PROMORDERID'], $row['ORDERNO'], $row['CODE1C'], $row['CALC'], $row['DATETIME']);
            array_push($arr, $e);
        }
        return $arr;
    }

    protected function Write1CSyncLogError($db, $id, $errText)
    {
        $sql = "update logs1c set error = ? where log1cid = ?";
        $q = ibase_prepare($db, $sql);
        if (!$q)
            throw new Exception(ibase_errmsg());
        if(!ibase_execute($q, $errText, $id))
            throw new Exception(ibase_errmsg());
    }

    protected function Update1CSyncLogProcessed($db, $id, $warnings)
    {
        $sql = "update logs1c set enddate = current_timestamp, error = nullif(cast(? as LARGENAME), '') where log1cid = ?";
        $q = ibase_prepare($db, $sql);
        if (!$q)
            throw new Exception(ibase_errmsg());
        if(!ibase_execute($q, $warnings, $id))
            throw new Exception(ibase_errmsg());
    }

    protected function Update1CQueueElementOK_Prepare($db)
    {
        $sql_accepted = "update queue m set m.datetime = current_timestamp, m.processed = 1
							  WHERE m.promorderid = ?
							  AND m.code1c = ?";
        $q = ibase_prepare($db, $sql_accepted);
        if (!$q)
            throw new Exception(ibase_errmsg());
        return $q;
    }

    protected function Update1CQueueElementFailed_Prepare($db)
    {
        $sql_error = "update queue m set m.result = ?
							 where m.promorderid = ?
							 and m.code1c = ?
							 and m.datetime is null";
        $q = ibase_prepare($db, $sql_error);
        if (!$q)
            throw new Exception(ibase_errmsg());
        return $q;
    }

    protected function Update1CQueueElementOK($db, $preparedQuery, $orderid, $marking)
    {
        if (!ibase_execute($preparedQuery, $orderid, $marking))
            throw new Exception(ibase_errmsg());
    }

    protected function Update1CQueueElementFailed($db, $preparedQuery, $orderid, $marking, $errText)
    {
        if (!ibase_execute($preparedQuery, $errText, $orderid,$marking))
            throw new Exception(ibase_errmsg());
    }
}


function Start1c($dbParam) {
    // получим конфигурацию
    $conf = SelectConf($dbParam);
    if ($conf != null) {
        $q = new T1CSkladQueue($conf->WSURL, $conf->WSUSERNAME, $conf->WSPASSWORD);
        try {
            $q->Process_1C_Queue($dbParam);
        }
        catch(Exception $e) {
            print $e->getTraceAsString();
        }
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
<?php

$prepareQuerys = array();

function Start($connectionStringParam, $userNameParam, $passwordParam, $logFileName) {
    $localDB = ibase_connect($connectionStringParam, $userNameParam, $passwordParam);
    if ($localDB) {
        try {
            LogSave("START", $logFileName);

            //Start1C($localDB);
            Process($localDB, $logFileName);

            LogSave("STOP", $logFileName);
        }
        catch (Exception $e) {
            LogSave($e->getMessage() . " Trace: " . $e->getTraceAsString(), $logFileName);
        }
    }
    else {
        LogSave("Не удалось установить соединение с БД. " . $connectionStringParam, $logFileName);
    }
}

function Process($localDBParam, $logFileNameParam) {
    $prom = ibase_pconnect('***', '***', '***');
    if ($prom) {
        $format = GetDateFormat();
        $shortFormat = GetShortDateFormat();
        $now = date($format);

        $query = "SELECT * FROM phpconf where isactive=1";
        $res = ibase_query($localDBParam, $query);
        if ($res) {
            // по executor-ам
            $row = ibase_fetch_object($res);
            while ($row != false) {
                if ($row->ISACTIVE === 1) {
                    $executorId = $row->EXECUTORID;
                    $getDataByDatePeriod = $row->GETBYDATEPERIOD;
                    $getDataByDateLastDateTime = $row->GETBYDATELASTDATETIME;
                    $getDataByIdPeriod = $row->GETBYIDPERIOD;
                    $getDataByIdLastDateTime = $row->GETBYDATESTOPDATETIME;
                    $getOnlyExtendedMaterial = $row->GETONLYEXTENDEDMATERIAL;

                    if ($getDataByDateLastDateTime == null or strtotime(date($format, strtotime( "-$getDataByDatePeriod minute" . $now ))) >= strtotime($getDataByDateLastDateTime)) {
                        $start = date($format);

                        for ($ii = -2; $ii <= 10; $ii++) {
                            $date = date($shortFormat, strtotime ( "$ii day" . $now ));

                            LogSave("Запрос данных из альты на $date", $logFileNameParam);
                            $data =  GetDataFromAltaByDateExecutor($prom, $date, $executorId, $getOnlyExtendedMaterial);
                            LogSave("Данные из альты получены", $logFileNameParam);

                            LogSave("Начало загрузки данных", $logFileNameParam);
                            SkladDBUpdate($localDBParam, $data, $logFileNameParam);
                            LogSave("Конец загрузки данных", $logFileNameParam);
                        }

                        $stop = date($format);
                        UpdatePHPConfByDate($localDBParam, $start, $stop, $executorId);
                    }

                    if ($getDataByIdLastDateTime == null or strtotime(date($format, strtotime( "-$getDataByIdPeriod minute" . $now ))) >= strtotime($getDataByIdLastDateTime)) {
                        $start = date($format);

                        $arrayNotProcess = GetNotProcessCacheBarcodesId($localDBParam);
                        if (count($arrayNotProcess) > 0 ) {

                            LogSave("Запрос данных из альты по списку необработаных штрихкодов", $logFileNameParam);
                            $data = GetDataFromAltaByBarcodesId($prom, $localDBParam, $arrayNotProcess, $getOnlyExtendedMaterial);
                            LogSave("Данные из альты получены", $logFileNameParam);

                            LogSave("Начало загрузки данных по необработаным штрихкодам", $logFileNameParam);
                            SkladDBUpdate($localDBParam, $data, $logFileNameParam);
                            ProcessBarcodesByBarcodesId($localDBParam, $arrayNotProcess);
                            LogSave("Конец загрузки данных", $logFileNameParam);
                        }

                        $stop = date($format);
                        UpdatePHPConfById($localDBParam, $start, $stop, $executorId);
                    }
                }

                $row = ibase_fetch_row($res);
            }
        }
        else {
            throw new Exception(ibase_errmsg());
        }
    }
    else {
        throw new Exception("Не удалось установить соедниение с БД Альты.");
    }
}

function UpdatePHPConfByDate($localDBParam, $startParam, $stopParam, $executorIdParam) {
    $query_text = "update phpconf set getbydatelastdatetime = ?, getbydatestopdatetime = ? where executorid = ?";
    $query = ibase_prepare($localDBParam, $query_text);
    if ($query) {
        $res = ibase_execute($query, $startParam, $stopParam, $executorIdParam);
        if ($res === false) {
            throw new Exception(ibase_errmsg());
        }
    }
    else {
        throw new Exception("Не удалось подготовить запрос. " . $query_text);
    }
}

function UpdatePHPConfById($localDBParam, $startParam, $stopParam, $executorIdParam) {
    $query_text = "update phpconf set getbyidlastdatetime = ?, getbyidstopdatetime = ? where executorid = ?";
    $query = ibase_prepare($localDBParam, $query_text);
    if ($query) {
        $res = ibase_execute($query, $startParam, $stopParam, $executorIdParam);
        if ($res === false) {
            throw new Exception(ibase_errmsg());
        }
    }
    else {
        throw new Exception("Не удалось подготовить запрос. " . $query_text);
    }
}

function GetDateFormat() {
    return "d.m.Y H:i:s";
}

function GetShortDateFormat() {
    return "d.m.Y";
}

// массив id необработанных штрихкодов
function GetUnknownCacheBarcodesId($localDBParam) {
    $ret  = Array();

    // бд склада
    // выберем id необработанных штрихкодов (processdatetime is null и docacceptelementid is null)
    $query_text = "select cachebarcodesid from cachebarcodes where docacceptelementid is null and elementid is null";
    $query = ibase_prepare($localDBParam, $query_text);
    if ($query) {
        $res = ibase_execute($query);
        if ($res) {
            $row = ibase_fetch_row($res);
            while ($row != false) {
                array_push($ret, $row[0]);
                $row = ibase_fetch_row($res);
            }

            return $ret;
        }
        else {
            throw new Exception(ibase_errmsg());
        }
    }
    else {
        throw new Exception("Не удалось подготовить запрос. " . $query_text);
    }
}

// Удаляет необработаные штрихкоды старше чем $dateParam
function DeleteOldNotProccesBarcodes($localDBParam, $transParam, $dateParam) {
    $query_text = "delete from cachebarcodes where processdatetime is null and docacceptelementid is null and datetime < ?";
    //$query = ibase_prepare($localDBParam, $transParam, $query_text);
    $query = PrepareQuery($localDBParam, $transParam, $query_text);
    if ($query) {
        $res = ibase_execute($query, $dateParam);
        if ($res === false) {
            throw new Exception(ibase_errmsg());
        }
    }
    else {
        throw new Exception("Не удалось подготовить запрос. " . $query_text);
    }
}

// массив id необработанных штрихкодов
//function GetNotProcessCacheBarcodesId($daysToSearchParam) {
function GetNotProcessCacheBarcodesId($localDBParam) {
    $ret = Array();

    $trans = ibase_trans($localDBParam);
    // выберем id необработанных штрихкодов (processdatetime is null и docacceptelementid is null)
    $query_text = "select cachebarcodesid from cachebarcodes where processdatetime is null and docacceptelementid is null";
    //$query = ibase_prepare($localDBParam, $query_text);
    $query = PrepareQuery($localDBParam, $trans, $query_text);
    if ($query) {
        $res = ibase_execute($query);
        if ($res)
        {
            $row = ibase_fetch_row($res);
            while ($row != false) {
                array_push($ret, $row[0]);
                $row = ibase_fetch_row($res);
            }

            return $ret;
        }
        else {
            throw new Exception(ibase_errmsg());
        }
    }
    else {
        throw new Exception("Не удалось подготовить запрос. " . $query_text);
    }
    ibase_commit($trans);
}

// массив штрихкодов по массиву cachebarcodesid
function GetBarcodeByCacheBarcodesId($localDBParam, $arrayCacheBarcodesIdParam) {
    $ret = Array();

    $trans = ibase_trans($localDBParam);
    foreach($arrayCacheBarcodesIdParam as $index => $item) {
        try {
            $queryBarcode_text = "select barcode from cachebarcodes where cachebarcodesid = ?";
            //$queryBarcode = ibase_prepare($localDBParam, $queryBarcode_text);
            $queryBarcode = PrepareQuery($localDBParam, $trans, $queryBarcode_text);
            if ($queryBarcode) {
                $resBarcode = ibase_execute($queryBarcode, $item);
                if ($resBarcode) {
                    $rowBarcode = ibase_fetch_row($resBarcode);

                    // хранимая процедура
                    $query_text = "select barcode from getbarcodebycachebarcode(?)";
                    //$query = ibase_prepare($localDBParam, $query_text);
                    $query = PrepareQuery($localDBParam, $trans, $query_text);
                    if ($query) {
                        $res = ibase_execute($query, $rowBarcode[0]);
                        if ($res) {
                            $row = ibase_fetch_row($res);
                            if ($row[0] != null) {
                                array_push($ret, $row[0]);
                            }
                        }
                        else {
                            throw new Exception(ibase_errmsg());
                        }
                    }
                    else {
                        throw new Exception("Не удалось подготовить запрос. " . $query_text);
                    }
                }
            }
            else {
                throw new Exception("Не удалось подготовить запрос. " . $queryBarcode_text);
            }
        }
        catch (Exception $e) {
            throw new Exception("Ошибка при получении штрихкодов");
        }
    }
    ibase_commit($trans);

    return array_unique($ret);
}

// обрабатывает все необработанные штрихкоды
function ProcessBarcodesByBarcodesId($localDBParam, $arrayCacheBarcodesIdParam)
{
    $trans = ibase_trans($localDBParam);
    // выберем id необработанных штрихкодов (processdatetime is null и docacceptelementid is null)
    foreach($arrayCacheBarcodesIdParam as $index => $item) {

        $query_text = 'select * from processbarcodebycachebarcodeid(' . $item . ')';
        //$query = ibase_prepare($localDBParam, $trans, $query_text);
        $query = PrepareQuery($localDBParam, $trans, $query_text);
        if ($query) {
            $res = ibase_execute($query);
            if ($res === false) {
                ibase_commit($trans);

                throw new Exception("ProcessBarcodesByBarcodesId. " . ibase_errmsg());
            }
        }
        else {
            ibase_commit($trans);

            throw new Exception("Не удалось подготовить запрос. " . $query_text);
        }
    }

    ibase_commit($trans);
}

// очищает временную таблицу рейсов
function ClearTempTrips($localDBParam, $transParam) {
    $query_text  = "delete from temptrips";
    //$query = ibase_prepare($localDBParam, $transParam, $query_text);
    $query = PrepareQuery($localDBParam, $transParam, $query_text);
    if ($query === false)
        throw new Exception("Не удалось подготовить запрос. " . $query_text);

    $res = ibase_execute($query);
    if ($res === false)
        throw new Exception(ibase_errmsg());
}

function SkladDBUpdate($localDBParam, $resAltaParam, $logFileNameParam) {
    $trans = ibase_trans($localDBParam);

    // массив key->value подготовленых запросов
    // key - текст запроса
    // value - подготовленый запрос
    $prepareQuerys = array();

    // очистим временную таблицу
    ClearTempTrips($localDBParam, $trans);
    // Удалим старые необработаные штрихкоды
    $oldDate = date(GetShortDateFormat(), strtotime ( "-7 day" . date(GetDateFormat())));
    DeleteOldNotProccesBarcodes($localDBParam, $trans, $oldDate);
    ibase_commit($trans);

    // есть массив $arrayBarcodes, он собирается с текущей базы склада (SelectBarcodes)
    // причем 0 элемент подмассива - штрихкод,
    //       1 элемент = ELEMENTID - если елемент из ELEMENTS впоследствии мождно удалить,
    //                = null - если элемент из ELEMENTS нельзя удалить
    // есть таблица $res из альты
    // алгоритм: если в $arrayBarcodes найден штрихкод из $res, удалим эту строку из $arrayBarcodes
    //          если в $arrayBarcodes не найден  штрихкод из $res, добавим этот штрихкод на склад
    //          в конечном счете в $arrayBarcodes останутся те штрихкоды, которые надо удалить со склада
    //          причем в нем же будут ссылки на строки ELEMENTS которые можно будет тоже удалить.
    //          удалим штрихкоды ($arrayBarcodes) со склада если их не обрабатывали (нет документа)
    //          удалим все "пустые" (по которым нет штрихкодов) строки в ELEMENTS
    //          удалим все "пустые" (по которым нет елементов) строки в ORDERS
    //          удалим все "пустые" (по которым нет заказов) строки в CUSTOMERS

    // массив для сравнения
    $arrayBarcodes = Array();

    // массив ключей 1с для заполнения ITEMS1C
    //$arrayKeys = Array();

    $row_alta = ibase_fetch_object($resAltaParam);

    // $trans = ibase_trans($localDBParam);
    // по executor-ам
    while ($row_alta != false) {
        $planDate = $row_alta->PLAN_DATE;
        $executorId = $row_alta->EXECUTORID;
        // заполним массив для сравнения
        $arrayBarcodes = SelectBarcodes($localDBParam, substr($planDate, 0, 10), $executorId);
        // по заказчикам
        while ($row_alta != false and $executorId == $row_alta->EXECUTORID) {
            $customerName = $row_alta->CUSTOMERNAME;
            $guidHi = $row_alta->CUSTOMER_GUIDHI;
            $guidLo = $row_alta->CUSTOMER_GUIDLO;
            $planDate = $row_alta->PLAN_DATE;

            $trans = ibase_trans($localDBParam);
            $customerId = UpdateOrInsertCustomer($localDBParam, $trans, $planDate, $customerName, $guidHi, $guidLo);

            // по заказам
            while ($row_alta != false and $customerName == $row_alta->CUSTOMERNAME) {
                $orderNo = $row_alta->ORDERNO;
                $ownerType = $row_alta->OWNERTYPE;
                $promOrderId = $row_alta->ORDERID;
                $diraction = $row_alta->DIR;
                $orderId = UpdateOrInsertOrder($localDBParam, $trans, $customerId, $orderNo, $executorId, $ownerType, $promOrderId, $diraction);

                // по элементам
                while ($row_alta != false and $orderNo == $row_alta->ORDERNO) {
                    $itemName =  $row_alta->ITEMFULLNAME;
                    $ctElementsId = $row_alta->CTELEMENTSID;
                    $remoteTripId = $row_alta->MYTRIPID;
                    $carName = $row_alta->CARNAME;
                    $driverName = $row_alta->DRIVERNAME;
                    $remoteOutlayId = $row_alta->CTWHOUTLAYID;
                    $waybill = $row_alta->WAYBILL;
                    $myApprovedate = $row_alta->MY_APPROVEDATE;
                    $myStatusProduced = $row_alta->MY_STATUS_PRODUCED;
                    $myStatusOk = $row_alta->MY_STATUS_OK;
                    $actionDate = $row_alta->ACTIONDATE;
                    $move1C = $row_alta->MOVE1C;
                    $code1C = $row_alta->CODE1C;
                    $key1C = $row_alta->ITEMKEY1C;
                    $rname = $row_alta->RNAME;
                    $tname = $row_alta->TNAME;

                    $item1cId = null;
                    // есть код в 1C?
                    if ($key1C != null) {
                        $item1cId = GetItem1cId($localDBParam, $trans, $code1C, $key1C);
                    }

                    $planDate = $row_alta->PLAN_DATE;
                    $elementId = UpdateOrInsertElements($localDBParam, $trans, $planDate, $ctElementsId, $itemName, $orderId,
                        $myStatusProduced, $myStatusOk, $actionDate, $move1C, $item1cId, $rname, $tname);

                    // запись во временной таблице temptrips для последующего добавления рейсов и накладных
                    InsertTempTrips($localDBParam, $trans, $remoteTripId, $carName, $driverName,
                        $remoteOutlayId, $waybill, $myApprovedate, $ctElementsId, $itemName);

                    // по по штрихкодам
                    //while ($row != false and $itemName == $row[5]) {
                    while ($row_alta != false and $ctElementsId == $row_alta->CTELEMENTSID) {
                        $barcode = $row_alta->BARCODE;
                        $move1C = $row_alta->MOVE1C;

                        // ищем штрихкод во временной таблице
                        $index = SearchBarcode($barcode, $arrayBarcodes);
                        // штрихкод не найден?						
                        if ($index === false) {
                            // добавим штрихкод на склад
                            $barcodeId = UpdateOrInsertBarcode($localDBParam, $trans, $elementId, $barcode);
                        }
                        else {
                            // штрихкод найден, значит он есть на складе. удалим его из массива
                            unset($arrayBarcodes[$index]);
                        }

                        // сигнальный элемент?
                        if ($move1C == 1) {
                            // обновим MOVE1C в ELEMENTS
                            //UpdateOrInsertElements($trans, $planDate, $ctElementsId, $itemName, $orderId,
                            //    $myStatusProduced, $myStatusOk, $actionDate, $move1C, $item1cId);

                            UpdateElementsMove1C($localDBParam, $trans, $ctElementsId, $move1C);
                        }

                        // следующая строка
                        $row_alta = ibase_fetch_object($resAltaParam);
                    }
                }
            }

            ibase_commit($trans);
        }
    }

    $trans = ibase_trans($localDBParam);
    // по штрихкодам, которые надо удалить со склада
    DeleteBarcodes($localDBParam, $trans, $arrayBarcodes);
    ibase_commit($trans);

    $trans = ibase_trans($localDBParam);
    // удалим пустые элементы заказы и заказчиков
    DeleteEmptyElementsOrdersCustomers($localDBParam, $trans);
    ibase_commit($trans);

    $trans = ibase_trans($localDBParam);
    // добавим рейсы и накладные из временной таблицы temptrips
    CreateTripsFromTempTable($localDBParam, $trans);
    ibase_commit($trans);

    $trans = ibase_trans($localDBParam);
    // удалим пустые рейсы и накладные
    DeleteEmptyTripsOutlays($localDBParam, $trans);
    ibase_commit($trans);
}

// по штрихкодам, которые надо удалить со склада
function DeleteBarcodes($localDBParam, $transParam, $arrayParam) {
    // по штрихкодам, которые надо удалить со склада
    foreach($arrayParam as $index => $item) {
        // можно удалить?
        if ($item[1] !== null) {
            // удалим штрихкод со склада
            DeleteBarcode($localDBParam, $transParam, $item[0]);
        }
    }
}

// удаляет пустые элементы, заказы и заказчиков
function DeleteEmptyElementsOrdersCustomers($localDBParam, $transParam) {
    // удалим пустые елементы
    //$query = ibase_prepare($localDBParam, $transParam, "delete from elements e where not exists
    //    (select * from barcodes b where b.elementid = e.elementid)");
    $query = PrepareQuery($localDBParam, $transParam, "delete from elements e where not exists
        (select * from barcodes b where b.elementid = e.elementid)");
    if ($query) {
        if (ibase_execute($query) === false) {
            throw new Exception(ibase_errmsg());
        }
    }
    else {
        throw new Exception("Не удалось подготовить запрос. " . $query);
    }

    // удалим пустые заказы
    //$query = ibase_prepare($localDBParam, $transParam, "delete from orders o where not exists
    //    (select * from elements e where o.orderid = e.orderid)");
    $query = PrepareQuery($localDBParam, $transParam, "delete from orders o where not exists
        (select * from elements e where o.orderid = e.orderid)");
    if ($query) {
        if (ibase_execute($query) === false) {
            throw new Exception(ibase_errmsg());
        }
    }
    else {
        throw new Exception("Не удалось подготовить запрос. " . $query);
    }

    // удалим пустых заказчиков
    //$query = ibase_prepare($localDBParam, $transParam, "delete from customers c where not exists
    //   (select * from orders o where c.customerid = o.customerid)");
    $query = PrepareQuery($localDBParam, $transParam, "delete from customers c where not exists
        (select * from orders o where c.customerid = o.customerid)");
    if ($query) {
        if (ibase_execute($query) === false) {
            throw new Exception(ibase_errmsg());
        }
    }
    else {
        throw new Exception("Не удалось подготовить запрос. " . $query);
    }
}

// удаляет пустые рейсы и накладные
function DeleteEmptyTripsOutlays($localDBParam, $transParam) {
    // удалим пустые рейсы
    //$query = ibase_prepare($localDBParam, $transParam, "delete from sendertrips t where not exists
    //    (select * from senderoutlays o where t.sendertripid = o.sendertripid)");
    $query = PrepareQuery($localDBParam, $transParam, "delete from sendertrips t where not exists
        (select * from senderoutlays o where t.sendertripid = o.sendertripid)");
    if ($query) {
        if (ibase_execute($query) === false) {
            throw new Exception(ibase_errmsg());
        }
    }
    else {
        throw new Exception("Не удалось подготовить запрос. " . $query);
    }

    // удалим пустые накладные
    //$query = ibase_prepare($localDBParam, $transParam, "delete from senderoutlays o where not exists
    //    (select * from elements e where e.senderoutlayid = o.senderoutlayid)");
    $query = PrepareQuery($localDBParam, $transParam, "delete from senderoutlays o where not exists
        (select * from elements e where e.senderoutlayid = o.senderoutlayid)");
    if ($query) {
        if (ibase_execute($query) === false) {
            throw new Exception(ibase_errmsg());
        }
    }
    else {
        throw new Exception("Не удалось подготовить запрос. " . $query);
    }
}

// добавим рейсы и накладные из временной таблицы temptrips
function CreateTripsFromTempTable($localDBParam, $transParam) {
    $temp = SelectTempTrips($localDBParam, $transParam);
    $row = ibase_fetch_row($temp);
    while ($row != false) {
        $remoteTripId = $row[0];
        $carName = $row[1];
        $driverName = $row[2];

        if ($remoteTripId != null) {
            $senderTripId = UpdateOrInsertSenderTrips($localDBParam, $transParam, $carName, $driverName, $remoteTripId);
        }
        else {
            $senderTripId = null;
        }

        while ($row != false and $remoteTripId == $row[0]) {
            $remoteOutlayId = $row[3];
            $waybill = $row[4];

            if ($remoteOutlayId != null) {
                $myApprovedate = $row[5];

                $senderOutlayId = UpdateOrInsertSenderOutLays($localDBParam, $transParam, $waybill, $myApprovedate, $senderTripId, $remoteOutlayId);
            }
            else {
                $senderOutlayId = null;
            }

            while ($row != false and $remoteOutlayId == $row[3]) {
                $ctElementsId = $row[6];

                if ($ctElementsId != null) {
                    UpdateElements($localDBParam, $transParam, $ctElementsId, $senderOutlayId);
                }

                while ($row != false and $ctElementsId == $row[6]) {
                    $row = ibase_fetch_row($temp);
                }
            }
        }
    }
}

function StrToData($strParam) {
    if($strParam != null )
        return date("Y-m-d H:i:s", strtotime($strParam));
    else
        return null;
}

function GetItem1cId($dbParam, $transParam, $code1cParam, $key1cParam) {
    $query_text = "select item1cid from items1c where key1c = ?";
    //$query = ibase_prepare($dbParam, $transParam, $query_text);
    $query = PrepareQuery($dbParam, $transParam, $query_text);
    if ($query) {
        $res = ibase_execute($query, $key1cParam);
        if ($res) {
            $row = ibase_fetch_row($res);

            if(empty($row)) {
                return InsertItems1c($dbParam, $transParam, $code1cParam, $key1cParam);
            }
            else {
                return $row[0];
            }
        }
        else {
            throw new Exception(ibase_errmsg());
        }
    }
    else {
        throw new Exception("Не удалось подготовить запрос. " . $query_text);
    }
}

function GetDataFromAlta($promDBParam, $getOnlyExtendedMaterilaParam) {

    // выгружать материалы?
    if ($getOnlyExtendedMaterilaParam == 1)
        $where = 'd.cttypeelemsid <> 1';
    else
        $where = '1=1';

    $query_alta_text = "with data as (
        select o.orderid
        , d.plan_date
        , ca.name as customername
        , c.guidhi as customer_guidhi
        , c.guidlo as customer_guidlo
        , o.orderno
        , e.executorid as executorid
        , o.ownertype
        , dd.name as dir
        from my_temp2 t2
        join orders o on o.orderid = t2.tmp_int
        join dirdates d on d.dirdatesid = o.my_dirdatesid
        join diractions dd on dd.diractionsid = d. diractionsid
        left join executors e on e.executorid = d.executorid
        left join customers c on o.customerid = c.customerid
        left join contragents ca on c.contragid = ca.contragid
        where c.customerid not in (22, 987)
        order by d.plan_date, customername, orderno
        )
        , data2 as
        (
        select d.executorid
        , d.plan_date
        , d.customername
        , d.orderid
        , d.orderno
        , d.ownertype
        , oi.name
            || iif(
                (select count(*) from models where orderitemsid = oi.orderitemsid) > 1
                , '.M'||(m.modelno + 1)
                , ''
            )|| iif(s.outlay_required = 1
                , '.'||iif(mp.sprpartid = 1, 'Р', 'С-' || mp.partnum)
                , ''
            )|| iif(oi.qty>1
                , '('||mi.itemno||'/'||oi.qty||')'
                , ''
            ) as itemfullname
        , m.my_1c_code||':'||mi.modelitemsid as itemkey1c -- объединяющий ключ для создания 1С-элементов
        , m.my_1c_code as code1c
        , iif(mp.sprpartid = 1, 1, 0) as move1c -- сигналим каждую раму - по ней будет происходить перемещение в 1С
        , coalesce(ead.ctelementsid, cte.ctelementsid) as ctelementsid
        , s.stvorkaid as barcode
        , customer_guidhi
        , customer_guidlo
        , mi.my_status_produced
        , mi.my_status_ok
        , ia.actiondate
        , r.name as rname
        , 1 as whid -- склад промышленая
        , cty.rname as tname
        , cte.cttypeelemsid
        , d.dir
        from data d
        left join orderitems oi on oi.orderid = d.orderid
        LEFT JOIN models m on oi.orderitemsid = m.orderitemsid
        LEFT JOIN modelitems mi on m.modelid = mi.modelid
        left join ct_elements cte on cte.my_modelitemsid = mi.modelitemsid
        JOIN my_stvorka s on mi.modelitemsid = s.modelitemsid
        left join my_itemadds ad on ad.misc_stvorkaid = s.stvorkaid
        left join ct_elements ead on ead.my_itemaddsid = ad.itemaddsid
        LEFT JOIN modelparts mp on s.modelpartid = mp.modelpartid
        left join itemaudit ia on ia.itemauditid = mi.my_status_produced
        left join r_systems r on r.rsystemid = m.my_profid
        left join CT_TYPEELEMS cty on cty.cttypeelemsid = cte.cttypeelemsid

        union all

        select
        d.executorid
        , d.plan_date
        , d.customername
        , d.orderid
        , d.orderno
        , d.ownertype
        , replace(cte.rname, orderno || '-', '') as itemfullname
        , iif(mi.modelitemsid is not null, m.my_1c_code || ':' || mi.modelitemsid, m.my_1c_code || ':' || cast(round(RAND() * 99999) as varchar(50))) as itemkey1c -- объединяющий ключ для создания 1С-элементов
        , m.my_1c_code as code1c
        , 1 -- сигналим каждую москитку
        , cte.ctelementsid
        , case
            when ms.msid is not null then
                '777'||right('000000000'||ms.orderitemsid, 9)
                ||right('00'||ms.modelno, 2)
                ||right(ms.sprpartid, 1)
                ||right('00'||ms.partnum, 2)
                ||right('00'||ms.itemno, 2)
            when cte.my_modelitemsid is not null and d.ownertype=1 then
                '888'||right('000000000'||cte.my_modelitemsid, 9)
            when cte.my_itemsdetailid is not null then
                '555'||right('000000000'||cte.ctelementsid, 9)
            when cte.ctelementsid is not null and cte.cttypeelemsid = 10 then
                '555'||right('000000000'||cte.ctelementsid, 9)
            else null
        end as barcode
        , customer_guidhi, customer_guidlo
        , mi.my_status_produced
        , mi.my_status_ok
        , ia.actiondate
        , r.name as rname
        , iif (det.goodsid is not null, (select first 1 wh.ctwarehousesid
            from goods g
            left join my_ct_goods_warehouses(oi.orderid, g.goodsid) wh on 1 = 1
            where g.goodsid = det.goodsid
        ), 1) as whid  -- 1-склад промышленая
        , cty.rname as tname
        , cte.cttypeelemsid
        , d.dir
        from data d
        left join ct_whorders who on who.orderid = d.orderid
        left join ct_whdetail whd on whd.ctwhordersid = who.ctwhordersid
        left join ct_elements cte on cte.ctelementsid = whd.ctelementsid
        left join itemsdetail det on det.itemsdetailid = cte.my_itemsdetailid
        left join groupgoods ggg on ggg.grgoodsid = cte.my_mygrgoodsid
        left join groupgoodstypes ggt on ggt.ggtypeid = ggg.ggtypeid
        left join my_ms ms on ms.msid = cte.my_msid
        left join orderitems oi on oi.orderid = d.orderid
        left join models m on oi.orderitemsid = m.orderitemsid
        left join modelitems mi on m.modelid = mi.modelid
        left join itemaudit ia on ia.itemauditid = mi.my_status_produced
        left join r_systems r on r.rsystemid = m.my_profid
        left join CT_TYPEELEMS cty on cty.cttypeelemsid = cte.cttypeelemsid
        where who.ctwarehousesid = 1
        and (cte.cttypeelemsid = 6 or cte.cttypeelemsid = 8 or cte.cttypeelemsid = 1)  -- в верхнем union'е my_stvorka джойнится жестко, поэтому СП мы там не получим, берём их здесь

        union all

        -- отливы хлебозаводская
        select
        d.executorid
        , d.plan_date
        , d.customername
        , d.orderid
        , d.orderno
        , d.ownertype
        , ce.rname itemfullname
        , iif(mi.modelitemsid is not null, m.my_1c_code || ':' || mi.modelitemsid, m.my_1c_code || ':' || cast(round(RAND() * 99999) as varchar(50))) itemkey1c -- объединяющий ключ для создания 1С-элементов
        , m.my_1c_code as code1c
        , 1
        , ce.ctelementsid
        , ce.ctelementsid as barcode
        , customer_guidhi
        , customer_guidlo
        , mi.my_status_produced
        , mi.my_status_ok
        , ia.actiondate
        , r.name as rname
        , iif (dt.goodsid is not null, (select first 1 wh.ctwarehousesid
            from goods g
            left join my_ct_goods_warehouses(oi.orderid, g.goodsid) wh on 1 = 1
            where g.goodsid = dt.goodsid
        ), 1) as whid  -- 1-склад промышленая
        , cty.rname as tname
        , ce.cttypeelemsid
        , d.dir
        from data d
        join orderitems oi on oi.orderid = d.orderid
        join itemsdetail dt on dt.orderitemsid = oi.orderitemsid
        join itemsdetail dt2 on dt2.my_parent = dt.itemsdetailid
        join goods g1 on g1.goodsid = dt.goodsid
        join goods g2 on g2.goodsid = dt2.goodsid
        join orderitems oi2 on oi2.orderitemsid = dt2.orderitemsid
        join ct_elements ce on ce.my_itemsdetailid = dt2.itemsdetailid
        left join models m on m.orderitemsid = oi.orderitemsid
        left join modelitems mi on mi.modelid = m.modelid
        left join itemaudit ia on ia.itemauditid = mi.my_status_produced
        left join r_systems r on r.rsystemid = m.my_profid
        left join ct_typeelems cty on cty.cttypeelemsid = ce.cttypeelemsid
        where d.executorid = 65 --хлебозаводская
        and ( g1.grgoodsid in (select zz.grgoodsid from my_exp_grgt zz where zz.pk_gt = 4))
        order by 1, 2, 3, 4

        )
        , data3 as (
        select d.*
        , (select first 1 oo.ctwhoutlayid
            from ct_whoutelements oe
            join ct_whoutorders oo on oo.ctwhoutordersid = oe.ctwhoutordersid
            where oe.ctelementsid = d.ctelementsid
            and oo.ctwarehousesid = 1
        ) as ctwhoutlayid
        from data2 d
        order by 1, 2, 3, 4
        )
        select d.plan_date
        , d.customername
        , d.orderid
        , d.orderno
        , trp.mytripid
        , out.ctwhoutlayid
        , d.itemfullname
        , d.itemkey1c
        , d.code1c
        , d.move1c
        , d.ctelementsid
        , d.barcode
        , d.customer_guidhi
        , d.customer_guidlo
        , out.waybill
        , out.my_approvedate
        , tro.mytripid as mytripid
        , cr.rname || ' ' || cr.num  as carname
        , drv.name as drivername
        , d.executorid
        , d.ownertype
        , d.my_status_produced
        , d.my_status_ok
        , d.actiondate
        , d.rname
        , d.tname
        , d.cttypeelemsid
        , d.dir
        from data3 d
        left join ct_whoutlay out on out.ctwhoutlayid = d.ctwhoutlayid
        left join my_tripoutlays tro on tro.ctwhoutlayid = out.ctwhoutlayid
        left join my_trip trp on tro.mytripid = trp.mytripid
        left join cars cr on trp.carsid = cr.carsid
        left join my_ct_drivers drv on trp.driverid = drv.driverid
        where d.whid = 1 -- склад промышленая
        and $where
        order by plan_date, executorid, customername, orderno, ctelementsid";

    $query = ibase_prepare($promDBParam, $query_alta_text);
    if ($query) {
        $res = ibase_execute($query);
        if ($res) {
            return $res;
        }
        else {
            throw new Exception(ibase_errmsg());
        }
    }
    else {
        throw new Exception("Не удалось подготовить запрос. " . substr($query_alta_text, 0, 30) . "... ");
    }
}

// обновляет бд склада по дате и executor-у
/**
 * @param $promDBParam
 * @param $dateTimeParam
 * @param $executorIdParam
 * @param $getOnlyExtendedMaterilaParam
 * @return resource
 * @throws Exception
 */
function GetDataFromAltaByDateExecutor($promDBParam, $dateTimeParam, $executorIdParam, $getOnlyExtendedMaterilaParam) {
    // добавим во временную таблицу заказы по дате и исполнителю.
    // чтобы получать данные из временной таблицы надо находится в
    // той же транзакции в которой происходит заполненеие временной таблицы
    // т.к. после закрытия транзакции временная таблица очищается

 
    $query_text = "insert into my_temp2 (tmp_int)
        select o.orderid
        from executors e
        join dirdates d on e.executorid = d.executorid
        join orders o on d.dirdatesid = o.my_dirdatesid
        where e.executorid = $executorIdParam
        and o.customerid not in (22, 987)
        and o.deleted is null
        and d.plan_date = '$dateTimeParam'
        group by orderid";

    $query = ibase_prepare($promDBParam, $query_text);
    if ($query) {
        $res = ibase_execute($query);
        if ($res) {
            return GetDataFromAlta($promDBParam, $getOnlyExtendedMaterilaParam);
        }
        else {
            throw new Exception(ibase_errmsg());
        }
    }
    else {
        throw new Exception("Не удалось подготовить запрос. " . $query_text);
    }
}

function GetDataFromAltaByBarcodesId($promDBParam, $localDBParam, $arrayBarcodesIdParam, $getOnlyExtendedMaterialParam) {
    // массив штрихкодов
    $arrayBarcodes = GetBarcodeByCacheBarcodesId($localDBParam, $arrayBarcodesIdParam);

    // массив заказов
    $arrayOrders = Array();

    // добавим во временную таблицу заказы,
    // которые соответствуют штрихкодам из массива $arrayBarcodesParam
    foreach($arrayBarcodes as $index => $item)
    {
        $isUpdate = false;
        $orderId = GetOrderByBarcode($promDBParam, $item);
        if ($orderId) {
            array_push($arrayOrders, $orderId);
        }
    }

    if (count($arrayOrders) > 0) {
        sort($arrayOrders);
        $ii = 0;
        while ($ii < count($arrayOrders) ) {
            $order = $arrayOrders[$ii];

            $query_text = "insert into my_temp2 (tmp_int) values (?)";
            $query = ibase_prepare($promDBParam, $query_text);
            if ($query) {
                $res = ibase_execute($query, $order);
                if ($res) {
                    while ($ii < count($arrayOrders) and $order == $arrayOrders[$ii]) {
                        $ii++;
                    }
                }
                else {
                    throw new Exception(ibase_errmsg());
                }
            }
            else {
                throw new Exception("Не удалось подготовить запрос. " . $query_text);
            }
        }

        return GetDataFromAlta($promDBParam, $getOnlyExtendedMaterialParam);
    }
}

function GenId($dbParam, $transParam, $generator) {
    $query_text = 'select gen_id(' . $generator . ', 1) from rdb$database';
    //$query = ibase_prepare($dbParam, $transParam, $query_text);
    $query = PrepareQuery($dbParam, $transParam, $query_text);
    if ($query) {
        $res = ibase_execute($query);
        if ($res) {
            $row = ibase_fetch_row($res);
            if (!empty($row))
                return $row[0];
            else
                throw new Exception("Не удалось сгенерировать Id. " . $generator);
        }
        else {
            throw new Exception(ibase_errmsg());
        }
    }
    else {
        throw new Exception("Не удалось подготовить запрос. " . $query_text);
    }
}

function InsertItems1c($dbParam, $transParam, $code1cParam, $key1cParam) {
    $id = GenId($dbParam, $transParam, "gen_items1c_id");

    $query_text = "insert into items1c (item1cid, code1c, key1c) values (?, ?, ?)";
    //$query = ibase_prepare($dbParam, $transParam, $query_text);
    $query = PrepareQuery($dbParam, $transParam, $query_text);
    if ($query) {
        $res = ibase_execute($query, $id, $code1cParam, $key1cParam);
        if ($res) {
            return $id;
        }
        else {
            throw new Exception(ibase_errmsg());
        }
    }
    else {
        throw new Exception("Не удалось подготовить запрос. " . $query_text);
    }
}

function UpdateOrInsertCustomer($dbParam, $transParam, $planDateParam, $nameParam, $guidHiParam, $guidLoParam){
    $query_text = "select customerid from customers where guidhi = ? and guidlo = ?";
    //$query = ibase_prepare($dbParam, $transParam, $query_text);
    $query = PrepareQuery($dbParam, $transParam, $query_text);
    if ($query) {
        $res = ibase_execute($query, $guidHiParam, $guidLoParam);
        if ($res) {
            $row = ibase_fetch_row($res);
            if (empty($row)) {
                $id = GenId($dbParam, $transParam, "gen_customers_id");

                $query_text = "insert into customers (customerid, plan_date, name, guidhi, guidlo) values (?, ?, ?, ?, ?)";
                //$query = ibase_prepare($dbParam, $transParam, $query_text);
                $query = PrepareQuery($dbParam, $transParam, $query_text);
                $res = ibase_execute($query, $id, $planDateParam, $nameParam, $guidHiParam, $guidLoParam);
                if ($res) {
                    return $id;
                }
                else {
                    throw new Exception(ibase_errmsg());
                }
            }
            else {
                $id = $row[0];
                $query_text = "update customers set plan_date = ?, name = ?, guidhi = ?, guidlo = ? where customerid = ?";
                //$query = ibase_prepare($dbParam, $transParam, $query_text);
                $query = PrepareQuery($dbParam, $transParam, $query_text);
                if ($query) {
                    $res = ibase_execute($query, $planDateParam, $nameParam, $guidHiParam, $guidLoParam, $id);
                    if ($res) {
                        return $id;
                    }
                    else {
                        throw new Exception(ibase_errmsg());
                    }
                }
                else {
                    throw new Exception("Не удалось подготовить запрос. " . $query_text);
                }
            }
        }
        else {
            throw new Exception(ibase_errmsg());
        }
    }
    else {
        throw new Exception("Не удалось подготовить запрос. " . $query_text);
    }
}

function InsertOrder($dbParam, $transParam, $customerIdParam, $orderNoParam, $executorIdParam, $ownerTypeParam) {
    $id = GenId($dbParam, $transParam, "gen_orders_id");

    $query_text = "insert into orders (orderid, customerid, orderno, executorid, ownertype)
        values (?, ?, ?, ?, ?)";
    $query = ibase_prepare($dbParam, $transParam, $query_text);
    if ($query) {
        $res = ibase_execute($query, $id, $customerIdParam, $orderNoParam, $executorIdParam, $ownerTypeParam);
        if ($res) {
            return $id;
        }
        else {
            throw new Exception(ibase_errmsg());
        }
    }
    else {
        throw new Exception("Не удалось подготовить запрос. " . $query_text);
    }
}

function UpdateOrInsertOrder($dbParam, $transParam, $customerIdParam, $orderNoParam, $executorIdParam, $ownerTypeParam, $promOrderId, $diraction) {
    $query_text = "select orderid from orders where orderno = ?";
    //$query = ibase_prepare($dbParam, $transParam, $query_text);
    $query = PrepareQuery($dbParam, $transParam, $query_text);
    if ($query) {
        $res = ibase_execute($query, $orderNoParam);
        if ($res) {
            $row = ibase_fetch_row($res);
            if (empty($row)) {
                $id = GenId($dbParam, $transParam, "gen_orders_id");

                $query_text = "insert into orders (orderid, customerid, orderno, executorid, ownertype, promorderid, dir) values (?, ?, ?, ?, ?, ?, ?)";
                //$query = ibase_prepare($dbParam, $transParam, $query_text);
                $query = PrepareQuery($dbParam, $transParam, $query_text);
                if ($query) {
                    $res = ibase_execute($query, $id, $customerIdParam, $orderNoParam, $executorIdParam, $ownerTypeParam, $promOrderId, $diraction);
                    if ($res) {
                        return $id;
                    }
                    else {
                        throw new Exception(ibase_errmsg());
                    }
                }
                else {
                    throw new Exception("Не удалось подготовить запрос. " . $query_text);
                }
            }
            else {
                $id = $row[0];
                $query_text = "update orders set customerid = ?, orderno = ?, executorid = ?, ownertype = ?, promorderid = ?, dir = ? where orderid = ?";
                //$query = ibase_prepare($dbParam, $transParam, $query_text);
                $query = PrepareQuery($dbParam, $transParam, $query_text);
                if ($query) {
                    $res = ibase_execute($query, $customerIdParam, $orderNoParam, $executorIdParam, $ownerTypeParam, $promOrderId, $diraction, $id);
                    if ($res) {
                        return $id;
                    }
                    else {
                        throw new Exception(ibase_errmsg());
                    }
                }
                else {
                    throw new Exception("Не удалось подготовить запрос. " . $query_text);
                }
            }
        }
        else {
            throw new Exception("UpdateOrInsertOrder. ".ibase_errmsg());
        }
    }
    else {
        throw new Exception("Не удалось подготовить запрос. " . $query_text);
    }
}

function InsertElement($dbParam, $transParam, $orderIdParam, $nameParam, $ctElementsIdParam) {
    $id = GenId($dbParam, $transParam, "gen_elements_id");

    $query_text = "insert into elements (elementid, orderid, name, ctelementsid) values (?, ?, ?, ?)";
    //$query = ibase_prepare($dbParam, $transParam, $query_text);
    $query = PrepareQuery($dbParam, $transParam, $query_text);
    if ($query) {
        $res = ibase_execute($query, $id, $orderIdParam, $nameParam, $ctElementsIdParam);
        if ($res) {
            return $id;
        }
        else {
            throw new Exception(ibase_errmsg());
        }
    }
    else {
        throw new Exception("Не удалось подготовить запрос. " . $query_text);
    }
}

function InsertBarcode($dbParam, $transParam, $elementIdParam, $barcodeParam) {
    $id = GenId($dbParam, $transParam, "gen_barcodes_id");

    $query_text = "insert into barcodes (barcodeid, elementid, barcode) values (?, ?, ?)";
    //$query = ibase_prepare($dbParam, $transParam, $query_text);
    $query = PrepareQuery($dbParam, $transParam, $query_text);
    if ($query) {
        $res = ibase_execute($query, $id, $elementIdParam, $barcodeParam);
        if ($res) {
            return $id;
        }
        else {
            throw new Exception(ibase_errmsg());
        }
    }
    else {
        throw new Exception("Не удалось подготовить запрос. " . $query_text);
    }
}

function UpdateOrInsertBarcode($dbParam, $transParam, $elementIdParam, $barcodeParam) {
    $query_text = "select barcodeid from barcodes where elementid = ? and barcode = ?";
    //$query = ibase_prepare($dbParam, $transParam, $query_text);
    $query = PrepareQuery($dbParam, $transParam, $query_text);
    if ($query) {
        $res = ibase_execute($query, $elementIdParam, $barcodeParam);
        if ($res) {
            $row = ibase_fetch_row($res);
            if (empty($row)) {
                $id = GenId($dbParam, $transParam, "gen_barcodes_id");

                $query_text = "insert into barcodes (barcodeid, elementid, barcode) values (?, ?, ?)";
                //$query = ibase_prepare($dbParam, $transParam, $query_text);
                $query = PrepareQuery($dbParam, $transParam, $query_text);
                if ($query) {
                    $res = ibase_execute($query, $id, $elementIdParam, $barcodeParam);
                    if ($res) {
                        return $id;
                    }
                    else {
                        throw new Exception(ibase_errmsg());
                    }
                }
                else {
                    throw new Exception("Не удалось подготовить запрос. " . $query_text);
                }
            }
            else {
                $id = $row[0];
                $query_text = "update barcodes set  elementid = ?, barcode = ? where barcodeid = ?";
                //$query = ibase_prepare($dbParam, $transParam, $query_text);
                $query = PrepareQuery($dbParam, $transParam, $query_text);
                if ($query) {
                    $res = ibase_execute($query, $elementIdParam, $barcodeParam, $id);
                    if ($res) {
                        return $id;
                    }
                    else {
                        throw new Exception(ibase_errmsg());
                    }
                }
                else {
                    throw new Exception("Не удалось подготовить запрос. " . $query_text);
                }
            }
        }
        else {
            throw new Exception(ibase_errmsg());
        }
    }
    else {
        throw new Exception("Не удалось подготовить запрос. " . $query_text);
    }
}

function UpdateOrInsertSenderOutLays($dbParam, $transParam, $wayBillParam, $my_approvedateParam, $senderTripIdParam, $remoteOutlayId) {
    $query_text = "select senderoutlayid from senderoutlays where remoteoutlayid = ?";
    //$query = ibase_prepare($dbParam, $transParam, $query_text);
    $query = PrepareQuery($dbParam, $transParam, $query_text);
    if ($query) {
        $res = ibase_execute($query, $remoteOutlayId);
        if ($res) {
            $row = ibase_fetch_row($res);
            if (empty($row)) {
                $id = GenId($dbParam, $transParam, "gen_senderoutlays_id");
                $query_text = "insert into senderoutlays (senderoutlayid, waybill, my_approvedate, sendertripid, remoteoutlayid)
                    values (?, ?, ?, ?, ?)";
                $query = ibase_prepare($dbParam, $transParam, $query_text);
                if ($query) {
                    $res = ibase_execute($query, $id, $wayBillParam, $my_approvedateParam, $senderTripIdParam, $remoteOutlayId);
                    if ($res) {
                        return $id;
                    }
                    else {
                        throw new Exception(ibase_errmsg());
                    }
                }
                else {
                    throw new Exception("Не удалось подготовить запрос. " . $query_text);
                }
            }
            else {
                $id = $row[0];

                $query_text = "update senderoutlays set waybill = ?, my_approvedate = ?, sendertripid =?, remoteoutlayid = ? where senderoutlayid = ?";
                //$query = ibase_prepare($dbParam, $transParam, $query_text);
                $query = PrepareQuery($dbParam, $transParam, $query_text);
                if ($query) {
                    $res = ibase_execute($query, $wayBillParam, $my_approvedateParam, $senderTripIdParam, $remoteOutlayId, $id);
                    if ($res) {
                        return $id;
                    }
                    else {
                        throw new Exception(ibase_errmsg());
                    }
                }
                else {
                    throw new Exception("Не удалось подготовить запрос. " . $query_text);
                }
            }
        }
        else {
            throw new Exception(ibase_errmsg());
        }
    }
    else {
        throw new Exception("Не удалось подготовить запрос. " . $query_text);
    }
}

function UpdateOrInsertSenderTrips($dbParam, $transParam, $carNameParam, $driverNameParam, $remoteTripIdParam) {
    $query_text = "select sendertripid from sendertrips where remotetripid = ?";
    //$query = ibase_prepare($dbParam, $transParam, $query_text);
    $query = PrepareQuery($dbParam, $transParam, $query_text);
    if ($query) {
        $res = ibase_execute($query, $remoteTripIdParam);
        if ($res) {
            $row = ibase_fetch_row($res);
            if (empty($row)) {
                $id = GenId($dbParam, $transParam, "gen_sendertrips_id");

                $query_text = "insert into sendertrips (sendertripid, carname, drivername, remotetripid)
                    values (?, ?, ?, ?)";
                //$query = ibase_prepare($dbParam, $transParam, $query_text);
                $query = PrepareQuery($dbParam, $transParam, $query_text);
                if ($query) {
                    $res = ibase_execute($query, $id, $carNameParam, $driverNameParam, $remoteTripIdParam);
                    if($res) {
                        return $id;
                    }
                    else {
                        throw new Exception(ibase_errmsg());
                    }
                }
                else {
                    throw new Exception("Не удалось подготовить запрос. " . $query_text);
                }
            }
            else {
                $id = $row[0];
                $query_text = "update sendertrips set carname = ?, drivername = ?, remotetripid = ? where sendertripid = ?";
                //$query = ibase_prepare($dbParam, $transParam, $query_text);
                $query = PrepareQuery($dbParam, $transParam, $query_text);
                if ($query) {
                    $res = ibase_execute($query, $carNameParam, $driverNameParam, $remoteTripIdParam, $id);
                    if ($res) {
                        return $id;
                    }
                    else {
                        throw new Exception(ibase_errmsg());
                    }
                }
                else {
                    throw new Exception("Не удалось подготовить запрос. " . $query_text);
                }
            }
        }
        else {
            throw new Exception(ibase_errmsg());
        }
    }
    else {
        throw new Exception("Не удалось подготовить запрос. " . $query_text);
    }
}

function UpdateElements($dbParam, $transParam, $ctElementsIdParam, $senderOutLayIdParam) {
    $query_text = "update elements set senderoutlayid = ? where ctelementsid = ?";
    //$query = ibase_prepare($dbParam, $transParam, $query_text);
    $query = PrepareQuery($dbParam, $transParam, $query_text);
    if ($query) {
        $res = ibase_execute($query, $senderOutLayIdParam, $ctElementsIdParam);
        if ($res === false) {
            throw new Exception(ibase_errmsg());
        }
    }
    else {
        throw new Exception("Не удалось подготовить запрос. " . $query_text);
    }
}

function UpdateElementsMove1C($dbParam, $trans, $ctElementsIdParam, $move1CParam) {
    $query_text = "update elements set move1c = ? where ctelementsid = ?";
    //$query = ibase_prepare($dbParam, $trans, $query_text);
    $query = PrepareQuery($dbParam, $trans, $query_text);
    if ($query) {
        $res = ibase_execute($query, $move1CParam, $ctElementsIdParam);

        //????????????????????????????????????????????????????????????????????????????
        if ($res === false) {
            throw new Exception(ibase_errmsg().$move1CParam.$ctElementsIdParam);
        }
    }
    else {
        throw new Exception("Не удалось подготовить запрос. " . $query_text);
    }
}

function UpdateOrInsertElements($dbParam, $transParam, $planDateParam, $ctElementsIdParam, $nameParam, $orderIdParam,
                                $myStatusProducedParam, $myStatusOkParam, $actionDateParam, $move1CParam, $item1cIdParam, $rnameParam, $typeNameParam) {
    $query_text = "select elementid from elements where ctelementsid = ?";
    //$query = ibase_prepare($dbParam, $transParam, $query_text);
    $query = PrepareQuery($dbParam, $transParam, $query_text);
    if ($query) {
        $res = ibase_execute($query, $ctElementsIdParam);
        if ($res) {
            $row = ibase_fetch_row($res);
            if (empty($row)) {
                $id = GenId($dbParam, $transParam, "gen_elements_id");

                $query_text = "insert into elements (elementid, plan_date, ctelementsid, name, orderid,
                    my_status_produced, my_status_ok, actiondate, move1c, item1cid, rname, typename)
                    values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                //$query = ibase_prepare($dbParam, $transParam, $query_text);
                $query = PrepareQuery($dbParam, $transParam, $query_text);
                if ($query) {
                    $res = ibase_execute($query,
                        $id, $planDateParam, $ctElementsIdParam, $nameParam, $orderIdParam, $myStatusProducedParam,
                        $myStatusOkParam, $actionDateParam, $move1CParam, $item1cIdParam, $rnameParam, $typeNameParam);

                    if ($res) {
                        return $id;
                    }
                    else {
                        throw new Exception(ibase_errmsg());
                    }
                }
                else {
                    throw new Exception("Не удалось подготовить запрос. " . $query_text);
                }
            }
            else {
                $id = $row[0];
                $query_text = "update elements set plan_date = ?, ctelementsid = ?, name = ?, orderid = ?,
                    my_status_produced = ?, my_status_ok = ?, actiondate = ?, move1c = ?, item1cid = ?, rname = ?, typename = ? where elementid = ?";
                //$query = ibase_prepare($dbParam, $transParam, $query_text);
                $query = PrepareQuery($dbParam, $transParam, $query_text);
                if ($query) {
                    $res = ibase_execute($query,
                        $planDateParam, $ctElementsIdParam, $nameParam, $orderIdParam, $myStatusProducedParam,
                        $myStatusOkParam, $actionDateParam, $move1CParam, $item1cIdParam, $rnameParam, $typeNameParam, $id);

                    if ($res) {
                        return $id;
                    }
                    else {
                        throw new Exception(ibase_errmsg());
                    }
                }
                else {
                    throw new Exception("Не удалось подготовить запрос. " . $query_text);
                }
            }
        }
        else {
            throw new Exception(ibase_errmsg());
        }
    }
    else {
        throw new Exception("Не удалось подготовить запрос. " . $query_text);
    }
}

function DeleteBarcode($dbParam, $transParam, $barcodeParam) {
    $query_text = "delete from barcodes where barcode = ?";
    //$query = ibase_prepare($dbParam, $transParam, $query_text);
    $query = PrepareQuery($dbParam, $transParam, $query_text);
    if ($query) {
        $res = ibase_execute($query, $barcodeParam);
        if ($res === false) {
            throw new Exception(ibase_errmsg());
        }
    }
    else {
        throw new Exception("Не удалось подготовить запрос. " . $query_text);
    }
}

function SelectBarcodes($dbParam, $planDateParam, $executorIdParam) {
    $query_text = "select b.barcode, max(e.elementid) as elementid, max(da.docacceptelementid) as docacceptelementid
        from customers c
        left join orders o on o.customerid = c.customerid
        left join elements e on e.orderid = o.orderid
        left join barcodes b on b.elementid = e.elementid
        left join docacceptelements da on da.elementid = e.elementid
        where c.plan_date = ? and o.executorid = ?
        group by barcode";
    $query = ibase_prepare($dbParam, $query_text);
    if ($query) {
        $res = ibase_execute($query, $planDateParam, $executorIdParam);
        if ($res) {
            $ii = 0;
            $array = array();
            $row = ibase_fetch_row($res);
            while ($row != false) {
                if ($row[2] == null) {
                    $array[$ii] = array($row[0], $row[1]);
                }
                else {
                    $array[$ii] = array($row[0], null);
                }

                $row = ibase_fetch_row($res);
                $ii++;
            }

            return $array;
        }
        else {
            throw new Exception(ibase_errmsg());
        }
    }
    else {
        throw new Exception("Не удалось подготовить запрос. " . $query_text);
    }
}

function SearchBarcode($barcodeParam, $arrayParam) {
    foreach($arrayParam as $index => $item)
    {
        if ($item[0] === $barcodeParam) {
            return $index;
        }
}

    return false;
}

function InsertTempTrips($dbParam, $transParam, $remoteTripIdParam, $carNameParam, $driverNameParam,
    $remoteOutlayIdParam, $waybillParam, $myApproveDateParam, $ctElementsIdParam, $nameParam) {

    $query_text = "insert into temptrips (remotetripid, carname, drivername, remoteoutlayid,
        waybill, my_approvedate, ctelementsid, name)
        values (?, ?, ?, ?, ?, ?, ?, ?)";
    //$query = ibase_prepare($dbParam, $transParam, $query_text);
    $query = PrepareQuery($dbParam, $transParam, $query_text);
    if ($query) {
        $res = ibase_execute($query, $remoteTripIdParam, $carNameParam, $driverNameParam, $remoteOutlayIdParam,
            $waybillParam, $myApproveDateParam, $ctElementsIdParam, $nameParam);

        if ($res === false) {
            throw new Exception(ibase_errmsg());
        }
    }
    else {
        throw new Exception("Не удалось подготовить запрос. " . $query_text);
    }
}

function SelectTempTrips($dbParam, $transParam) {
    $query_text = "select * from temptrips order by remotetripid, remoteoutlayid, ctelementsid";
    $query = ibase_prepare($dbParam, $transParam, $query_text);
    if ($query) {
        $res = ibase_execute($query);

        if ($res) {
            return $res;
        }
        else {
            throw new Exception(ibase_errmsg());
        }
    }
    else {
        throw new Exception("Не удалось подготовить запрос. " . $query_text);
    }
}

// возвращает id заказа по штрихкоду (Рома)
function GetOrderByBarcode($dbParam, $barcodeParam) {
    if (preg_match("/^777(\d{9})(\d{2})(\d)(\d{2})(\d{2})$/", $barcodeParam, $matches)) {
        $CodeType = 'ms';
        $orderitemsid = $matches[1];
        $modelno = $matches[2];
        $sprpartid = $matches[3];
        $partnum = $matches[4];
        $itemno = $matches[5];
        $sql = "select first 1 who.orderid
			from my_ms ms
			join ct_elements e on e.my_msid = ms.msid
			join ct_whdetail whd
					join ct_whorders who on who.ctwhordersid = whd.ctwhordersid and who.ctwarehousesid = 1
				on whd.ctelementsid = e.ctelementsid
			where ms.orderitemsid = $orderitemsid
			and ms.modelno = $modelno
			and ms.sprpartid = $sprpartid
			and ms.partnum = $partnum
			and ms.itemno = $itemno";
    }
    else if (preg_match("/^888(\d{9})$/", $barcodeParam, $matches)) {
        $CodeType = 'gp';
        $modelitemsid = $matches[1];
        $sql = "select first 1 who.orderid
            from ct_elements e
			join ct_whdetail whd
					join ct_whorders who on who.ctwhordersid = whd.ctwhordersid and who.ctwarehousesid = 1
				on whd.ctelementsid = e.ctelementsid
            where e.my_modelitemsid = $modelitemsid
            and e.cttypeelemsid = 6";
    }
    else if (preg_match("/^555(\d{9})$/", $barcodeParam, $matches)) {
        $CodeType = 'dop';
        $cte = $matches[1];
        $sql = "select first 1 who.orderid
			from ct_elements e
			join ct_whdetail whd
					join ct_whorders who on who.ctwhordersid = whd.ctwhordersid and who.ctwarehousesid = 1
				on whd.ctelementsid = e.ctelementsid
			where e.ctelementsid = $cte
			and e.cttypeelemsid in (1, 9, 10)";
    }
    else if (preg_match("/^.*?(\d{1,9})$/", $barcodeParam, $matches)) {
        $CodeType = 'izd';
        $stvorkaid = $matches[1];
        $sql = "select first 1 who.orderid
			, mi.modelitemsid
			from my_stvorka s
			left join ct_elements e on e.my_modelitemsid = s.modelitemsid
			join ct_whdetail whd
					join ct_whorders who on who.ctwhordersid = whd.ctwhordersid and who.ctwarehousesid = 1
				on whd.ctelementsid = e.ctelementsid
			left join modelitems mi on mi.modelitemsid = e.my_modelitemsid
            left join models m on m.modelid = mi.modelid
            left join orderitems i on i.orderitemsid = m.orderitemsid
			where s.stvorkaid = $stvorkaid
			and e.cttypeelemsid = 2
			and who.orderid is not null
			order by e.ctelementsid desc";
    }
    else {
        return false;
    }

    $query = ibase_prepare($dbParam, $sql);
    if ($query) {
        $res = ibase_execute($query);

        if ($res) {
            $row = ibase_fetch_row($res);
            if (empty($row)) {
                return false;
            }
            else
            {
                return $row[0];
            }
        }
        else {
            throw new Exception(ibase_errmsg());
        }
    }
    else {
        throw new Exception("Не удалось подготовить запрос. " . $sql);
    }
}

function LogSave($logText, $logFileName)
{
    //$home_dir = str_replace($_SERVER['DOCUMENT_ROOT'], "", str_replace("\\", '/', getcwd()));
    //$real_log_dir = $_SERVER['DOCUMENT_ROOT'] . $home_dir;
    //$real_log_dir = GetCurrentDir();
    //chmod($real_log_dir, 0777);
    //$h = fopen($real_log_dir . "/" . $logFileName , "a+");

    $h = fopen(getcwd() . "/" . $logFileName , "a+");
    fwrite($h, date(GetDateFormat()) . " ". $logText . "\r\n");
    fclose($h);
}

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
                            //while ($row != false) {
                            //    // есть id заказа? (без id старые заказы, просто пропустим)
                            //    if ($row->PROMORDERID != null) {
                            //        $XMLRow = $object->addChild('row');
                            //        $XMLRow['datecreated']=date('Y-m-d H:i:s', strtotime($row->DATETIME));
                            //       if ($row->SENDER !== '') {
                            //            $XMLRow['warehouse_sender'] = iconv('windows-1251', 'utf-8', $row->SENDER);
                            //        }
                            //        if($row->RECIPIENT !== '') {
                            //            $XMLRow['warehouse_recipient'] = iconv('windows-1251', 'utf-8', $row->RECIPIENT);
                            //        }
                            //        $XMLRow['orderid'] = $row->PROMORDERID;
                            //        $XMLRow['zayavka'] = iconv('windows-1251', 'utf-8', $row->ORDERNO);
                            //         $XMLRow['marking'] = $row->CODE1C;
                            //         $XMLRow['calc'] = $row->CALC;
                            //         $XMLRow['queueid'] = $row->QUEUEID;

                            // запись в промежуточной таблице для связи один ко многим таблиц QUEUE и LOGS1C
                            //            InsertQueueLogs1c($dbParam, $row->QUEUEID, $Log1cId);
                            //        }
                            //       $row = ibase_fetch_object($res);
                            //        $ii++;
//
//                                if ($ii = 10) {
//                                    break;
//                                }
//                            }

                            $ii = 10;
                            if ($ii > 0) {

                                $req = '<object><query_text>ВЫБРАТЬ Спр.Наименование,Спр.Код ИЗ Справочник.Пользователи КАК Спр ГДЕ НЕ Спр.ПометкаУдаления</query_text><query_rez>Строка</query_rez><parameters></parameters></object>';

                                $params = array(
                                    "param1" => "execute_query_1c",
                                    "param2" => $req
                                );

                                //$client = new SoapClient("http://dev1c/steklodom/ws/SuperWS.1cws?wsdl", array("login"=>"WSclient", "password"=>"masterkey", "trace" => 1));
                                $client = new SoapClient("http://localhost/test.php", array("login"=>"WSclient", "password"=>"masterkey", "trace" => 1));
                                $res = $client->djinn($params);

                                $xxx = $client->__getLastRequest();


                                $xml = new SimpleXMLElement($res->param2);
                                foreach ($xml as $el) {

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

function PrepareQuery($dbParam, $dbTransParam, $queryParam) {
    try {
        $prep =  GetPrepareQueryFormArray($dbTransParam, $queryParam);
        if (!$prep)
        {
            $new_prepare = ibase_prepare($dbParam, $dbTransParam, $queryParam);

            $idTrans = (string)$dbTransParam;
            $part = array("$idTrans # $queryParam" => $new_prepare);
            $GLOBALS['prepareQuerys'] = $GLOBALS['prepareQuerys'] + $part;

            return $new_prepare;
        }
        else {
            return $prep;
        }
    }
    catch (Exception $e) {
        throw $e;
    }
}

function GetPrepareQueryFormArray($dbTransParam, $queryParam) {
    $idTrans = (string)$dbTransParam;
    $key = "$idTrans # $queryParam";
    if (array_key_exists($key, $GLOBALS['prepareQuerys'])) {
        return $GLOBALS['prepareQuerys'][$key];
    }
    else {
        return false;
    }
}

?>
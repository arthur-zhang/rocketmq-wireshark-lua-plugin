package.prepend_path("rocketmq-plugin")
local json = dofile(DATA_DIR.."rocketmq-plugin/json.lua")

local requestCodeMap = {
    [10] = "SEND_MESSAGE",
    [11] = "PULL_MESSAGE",
    [12] = "QUERY_MESSAGE",
    [13] = "QUERY_BROKER_OFFSET",
    [14] = "QUERY_CONSUMER_OFFSET",
    [15] = "UPDATE_CONSUMER_OFFSET",
    [17] = "UPDATE_AND_CREATE_TOPIC",
    [21] = "GET_ALL_TOPIC_CONFIG",
    [22] = "GET_TOPIC_CONFIG_LIST",
    [23] = "GET_TOPIC_NAME_LIST",
    [25] = "UPDATE_BROKER_CONFIG",
    [26] = "GET_BROKER_CONFIG",
    [27] = "TRIGGER_DELETE_FILES",
    [28] = "GET_BROKER_RUNTIME_INFO",
    [29] = "SEARCH_OFFSET_BY_TIMESTAMP",
    [30] = "GET_MAX_OFFSET",
    [31] = "GET_MIN_OFFSET",
    [32] = "GET_EARLIEST_MSG_STORETIME",
    [33] = "VIEW_MESSAGE_BY_ID",
    [34] = "HEART_BEAT",
    [35] = "UNREGISTER_CLIENT",
    [36] = "CONSUMER_SEND_MSG_BACK",
    [37] = "END_TRANSACTION",
    [38] = "GET_CONSUMER_LIST_BY_GROUP",
    [39] = "CHECK_TRANSACTION_STATE",
    [40] = "NOTIFY_CONSUMER_IDS_CHANGED",
    [41] = "LOCK_BATCH_MQ",
    [42] = "UNLOCK_BATCH_MQ",
    [43] = "GET_ALL_CONSUMER_OFFSET",
    [45] = "GET_ALL_DELAY_OFFSET",
    [100] = "PUT_KV_CONFIG",
    [101] = "GET_KV_CONFIG",
    [102] = "DELETE_KV_CONFIG",
    [103] = "REGISTER_BROKER",
    [104] = "UNREGISTER_BROKER",
    [105] = "GET_ROUTEINTO_BY_TOPIC",
    [106] = "GET_BROKER_CLUSTER_INFO",
    [200] = "UPDATE_AND_CREATE_SUBSCRIPTIONGROUP",
    [201] = "GET_ALL_SUBSCRIPTIONGROUP_CONFIG",
    [202] = "GET_TOPIC_STATS_INFO",
    [203] = "GET_CONSUMER_CONNECTION_LIST",
    [204] = "GET_PRODUCER_CONNECTION_LIST",
    [205] = "WIPE_WRITE_PERM_OF_BROKER",
    [206] = "GET_ALL_TOPIC_LIST_FROM_NAMESERVER",
    [207] = "DELETE_SUBSCRIPTIONGROUP",
    [208] = "GET_CONSUME_STATS",
    [209] = "SUSPEND_CONSUMER",
    [210] = "RESUME_CONSUMER",
    [211] = "RESET_CONSUMER_OFFSET_IN_CONSUMER",
    [212] = "RESET_CONSUMER_OFFSET_IN_BROKER",
    [213] = "ADJUST_CONSUMER_THREAD_POOL",
    [214] = "WHO_CONSUME_THE_MESSAGE",
    [215] = "DELETE_TOPIC_IN_BROKER",
    [216] = "DELETE_TOPIC_IN_NAMESRV",
    [219] = "GET_KVLIST_BY_NAMESPACE",
    [220] = "RESET_CONSUMER_CLIENT_OFFSET",
    [221] = "GET_CONSUMER_STATUS_FROM_CLIENT",
    [222] = "INVOKE_BROKER_TO_RESET_OFFSET",
    [223] = "INVOKE_BROKER_TO_GET_CONSUMER_STATUS",
    [300] = "QUERY_TOPIC_CONSUME_BY_WHO",
    [224] = "GET_TOPICS_BY_CLUSTER",
    [301] = "REGISTER_FILTER_SERVER",
    [302] = "REGISTER_MESSAGE_FILTER_CLASS",
    [303] = "QUERY_CONSUME_TIME_SPAN",
    [304] = "GET_SYSTEM_TOPIC_LIST_FROM_NS",
    [305] = "GET_SYSTEM_TOPIC_LIST_FROM_BROKER",
    [306] = "CLEAN_EXPIRED_CONSUMEQUEUE",
    [307] = "GET_CONSUMER_RUNNING_INFO",
    [308] = "QUERY_CORRECTION_OFFSET",
    [309] = "CONSUME_MESSAGE_DIRECTLY",
    [310] = "SEND_MESSAGE_V2",
    [311] = "GET_UNIT_TOPIC_LIST",
    [312] = "GET_HAS_UNIT_SUB_TOPIC_LIST",
    [313] = "GET_HAS_UNIT_SUB_UNUNIT_TOPIC_LIST",
    [314] = "CLONE_GROUP_OFFSET",
    [315] = "VIEW_BROKER_STATS_DATA",
    [316] = "CLEAN_UNUSED_TOPIC",
    [317] = "GET_BROKER_CONSUME_STATS",
    [318] = "UPDATE_NAMESRV_CONFIG",
    [319] = "GET_NAMESRV_CONFIG",
    [320] = "SEND_BATCH_MESSAGE",
    [321] = "QUERY_CONSUME_QUEUE",
    [322] = "QUERY_DATA_VERSION",
}

local responseCode = {
    [0] = "SUCCESS",
    [1] = "SYSTEM_ERROR",
    [2] = "SYSTEM_BUSY",
    [3] = "REQUEST_CODE_NOT_SUPPORTED",
    [4] = "TRANSACTION_FAILED",
    [10] = "FLUSH_DISK_TIMEOUT",
    [11] = "SLAVE_NOT_AVAILABLE",
    [12] = "FLUSH_SLAVE_TIMEOUT",
    [13] = "MESSAGE_ILLEGAL",
    [14] = "SERVICE_NOT_AVAILABLE",
    [15] = "VERSION_NOT_SUPPORTED",
    [16] = "NO_PERMISSION",
    [17] = "TOPIC_NOT_EXIST",
    [18] = "TOPIC_EXIST_ALREADY",
    [19] = "PULL_NOT_FOUND",
    [20] = "PULL_RETRY_IMMEDIATELY",
    [21] = "PULL_OFFSET_MOVED",
    [22] = "QUERY_NOT_FOUND",
    [23] = "SUBSCRIPTION_PARSE_FAILED",
    [24] = "SUBSCRIPTION_NOT_EXIST",
    [25] = "SUBSCRIPTION_NOT_LATEST",
    [26] = "SUBSCRIPTION_GROUP_NOT_EXIST",
    [200] = "TRANSACTION_SHOULD_COMMIT",
    [201] = "TRANSACTION_SHOULD_ROLLBACK",
    [202] = "TRANSACTION_STATE_UNKNOW",
    [203] = "TRANSACTION_STATE_GROUP_WRONG",
    [204] = "NO_BUYER_ID",
    [205] = "NOT_IN_CURRENT_UNIT",
    [206] = "CONSUMER_NOT_ONLINE",
    [207] = "CONSUME_MSG_TIMEOUT",
    [208] = "NO_MESSAGE",
}

local PORTS = { 9876, 10911, 20111 }

function isUpDirection(dstPort)
    for _, port in ipairs(PORTS) do
        if (dstPort == port) then
            return true
        end
    end
    return false
end

local NAME = "RocketMQ"
local protoMQ = Proto.new(NAME, "RocketMQ Protocol")

function decodeMessageExt(bodyTree, pinfo, bodyData)
    local bodyTree = bodyTree:add("Body", "")

    pinfo.cols.info:append(">>>>#FOUND#")

    local offset = 0;

    bodyTree:add("totalSize", bodyData(offset, 4):int())
    offset = offset + 4;

    local magicCode = string.format("0X%8.8X", bodyData(offset, 4):uint())
    bodyTree:add("magicCode", magicCode)
    offset = offset + 4;

    bodyTree:add("bodyCRC", bodyData(offset, 4):int())
    offset = offset + 4;

    bodyTree:add("queueId", bodyData(offset, 4):int())
    offset = offset + 4;

    bodyTree:add("flag", bodyData(offset, 4):int())
    offset = offset + 4;

    bodyTree:add("queueOffset", bodyData(offset, 8):int64():tonumber())
    offset = offset + 8;

    bodyTree:add("physicOffset", bodyData(offset, 8):int64():tonumber())
    offset = offset + 8;

    bodyTree:add("sysFlag", bodyData(offset, 4):int())
    offset = offset + 4;


    bodyTree:add("bornTimeStamp", bodyData(offset, 8):int64():tonumber())
    offset = offset + 8;

    local bornHost = bodyData(offset, 1):uint()
            .. "." .. bodyData(offset + 1, 1):uint()
            .. "." .. bodyData(offset + 2, 1):uint()
            .. "." .. bodyData(offset + 3, 1):uint()

    bodyTree:add("bornHost", bornHost)
    offset = offset + 4;

    bodyTree:add("port", bodyData(offset, 4):int())
    offset = offset + 4;
    bodyTree:add("storeTimestamp", bodyData(offset, 8):int64():tonumber())
    offset = offset + 8;

    local storeHost = bodyData(offset, 1):uint()
            .. "." .. bodyData(offset + 1, 1):uint()
            .. "." .. bodyData(offset + 2, 1):uint()
            .. "." .. bodyData(offset + 3, 1):uint()
    bodyTree:add("storeHost", storeHost)
    offset = offset + 4;

    bodyTree:add("storePort", bodyData(offset, 4):int())
    offset = offset + 4;

    --13 RECONSUMETIMES
    bodyTree:add("reconsumeTimes", bodyData(offset, 4):int())
    offset = offset + 4;
    --14 Prepared Transaction Offset
    bodyTree:add("preparedTransactionOffset", bodyData(offset, 8):int64():tonumber())
    offset = offset + 8;
    --15 BODY
    local bodyLen = bodyData(offset, 4):int()
    --            bodyTree:add("bodyLen", bodyLen)
    offset = offset + 4;

    bodyTree:add("body:", bodyData(offset, bodyLen):string())
    offset = offset + bodyLen;

    --16 TOPIC
    local topicLen = bodyData(offset, 1):int()
    offset = offset + 1;
    --            bodyTree:add("topicLen", topicLen)
    local topic = bodyData(offset, topicLen):string()
    bodyTree:add("topic:", topic)
    pinfo.cols.info:append(" topic:" .. topic)

    offset = offset + topicLen;

    --17 properties
    local propertiesLength = bodyData(offset, 2):int()
    offset = offset + 2;
    bodyTree:add("propertiesLength", propertiesLength)

    if (propertiesLength > 0) then
        local propertiesStr = bodyData(offset, propertiesLength):string()
        offset = offset + propertiesLength
        local propertiesTree = bodyTree:add("propertiesStr", "size: " .. propertiesLength)
        for k, v in string.gmatch(propertiesStr, "(%w+)\1(%w+)") do
            propertiesTree:add(k, v)
        end
    end
end

function parseAndAddTree(k, v, tree)
    if (type(v) == 'table') then
        local sizeStr = ""
        if (#v > 0) then
            sizeStr = "size: " .. #v
        end;
        local childTree = tree:add(k, sizeStr, tree)
        for key, value in pairs(v) do
            parseAndAddTree(key, value, childTree)
        end
    else
        tree:add(k .. ":", json.stringify(v))
    end
end


function protoMQ.dissector(tvb, pinfo, tree)
    local srcPort = pinfo.src_port;
    local dstPort = pinfo.dst_port;

    local subtree = tree:add(protoMQ, tvb())
    pinfo.cols.protocol = protoMQ.name;
    pinfo.cols.info = ""

    local length = tvb(0, 4):uint()

    subtree:add("Length", length)
    local headerLength = tvb(4, 4):uint()
    local headerData = tvb(8, headerLength):string()
    local header = json.parse(headerData, 1, "}")
    local headerTree = subtree:add("Header", "")

    local isRemarkFound = false

    if (isUpDirection(dstPort)) then
        --        request
        pinfo.cols.info:append("[REQUEST]" .. "↑↑↑")
        for k, v in pairs(header) do
            if (k == "code") then
                local codeStr = requestCodeMap[v];
                if (codeStr == nil) then
                    break;
                end
                pinfo.cols.info:append(" code=" .. v .. "(" .. codeStr .. ")")
            end
        end
        for k, v in pairs(header) do
            if (k == "extFields") then
                if (v["topic"] ~= nil) then
                    pinfo.cols.info:append(" topic:" .. v["topic"])
                end
            end
        end
    else
        --        response
        pinfo.cols.info:append("[RESPONSE]" .. "↓↓↓")
        for k, v in pairs(header) do
            if (k == "code") then
                local codeStr = responseCode[v];
                if (codeStr == nil) then
                    break;
                end
                pinfo.cols.info:append(" code=" .. v .. "(" .. codeStr .. ")")
            end
        end
        local remark = header["remark"]

        if (remark ~= nil and remark == "FOUND") then
            isRemarkFound = true
        end
    end

    for k, v in pairs(header) do
        parseAndAddTree(k, v, headerTree)
    end

    local bodyDataLen = tvb:len() - 8 - headerLength

    if (bodyDataLen <= 0) then
        return
    end
    local bodyData = tvb(8 + headerLength, bodyDataLen)

    if (bodyData ~= nil and bodyData:len() > 0) then
        if (not isRemarkFound) then
            bodyData = bodyData:string()
            if (bodyData:sub(1, 1) == '{' or bodyData:sub(1, 1) == '[') then
                local body = json.parse(bodyData, 1, "}")
                parseAndAddTree("Body", body, subtree)
            else
                subtree:add("Body:", bodyData)
            end
        else
            decodeMessageExt(subtree, pinfo, bodyData)
        end
    end
end


for _, port in ipairs(PORTS) do
    DissectorTable.get("tcp.port"):add(port, protoMQ)
end

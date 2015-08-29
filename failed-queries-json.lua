-----------------------------------------------------------------------------
-- A LUA script for MySQL proxy to output failed SQL queries as JSON
--
-- Version: 0.3
-- This script is released under the MIT License (MIT).
-- Please see LICENCE.txt for details.
--
-- REQUIREMENTS:
--   JSON module (https://github.com/craigmj/json4lua)
-----------------------------------------------------------------------------
local proto = require("mysql.proto")

function read_query(packet)
    local tp = packet:byte()

    if tp == proxy.COM_QUERY or tp == proxy.COM_STMT_PREPARE or tp == proxy.COM_STMT_EXECUTE or tp == proxy.COM_STMT_CLOSE then
        proxy.queries:append(1, packet, { resultset_is_needed = true } )
        return proxy.PROXY_SEND_QUERY
    end
end

local prepared_stmts = { }

function read_query_result(inj)
    local tp     =    string.byte(inj.query)
    local res    =    assert(inj.resultset)
    local query  =    ""

    -- Preparing statement...
    if tp == proxy.COM_STMT_PREPARE then
        if inj.resultset.raw:byte() == 0 then
            local stmt_prepare      =    assert(proto.from_stmt_prepare_packet(inj.query))
            local stmt_prepare_ok   =    assert(proto.from_stmt_prepare_ok_packet(inj.resultset.raw))

            prepared_stmts[stmt_prepare_ok.stmt_id] = {
              query           =    stmt_prepare.stmt_text,
              num_columns     =    stmt_prepare_ok.num_columns,
              num_params      =    stmt_prepare_ok.num_params,
            }
        end
    elseif tp == proxy.COM_STMT_CLOSE then -- ...closing statement
        local stmt_close = assert(proto.from_stmt_close_packet(inj.query))

        prepared_stmts[stmt_close.stmt_id] = nil -- cleaning up
    end

    -- An error occured...
    if(res.query_status == proxy.MYSQLD_PACKET_ERR or res.warning_count > 0) then
        -- ... while executing a prepared statement
        if tp == proxy.COM_STMT_EXECUTE then
            local stmt_id        =    assert(proto.stmt_id_from_stmt_execute_packet(inj.query))
            local stmt_execute      =    assert(proto.from_stmt_execute_packet(inj.query, prepared_stmts[stmt_id].num_params))
            query             =    prepared_stmts[stmt_id].query

            if stmt_execute.new_params_bound then
                local params    =    ""

                for ndx, v in ipairs(stmt_execute.params) do
                    params  =    params .. ("[%d] %s (type = %d) "):format(ndx, tostring(v.value), v.type)
                end

                query   =    query .. " - " .. params
            end
        elseif tp == proxy.COM_QUERY then -- ... or an exec() query
            query        =    string.sub(inj.query, 2)
        else
            do return end
        end

        local err_code    =    res.raw:byte(2) + (res.raw:byte(3) * 256)

        if(err_code ~= 0) and (err_code ~= 1) then

            local err_sqlstate =    res.query_status == proxy.MYSQLD_PACKET_ERR and res.raw:sub(5, 9) or 0
            local err_msg      =    res.query_status == proxy.MYSQLD_PACKET_ERR and res.raw:sub(10) or ''

            local objDt    =    {
                user    =    proxy.connection.server.username,
                db      =    proxy.connection.server.default_db,
                query   =    query,
                msg     =    err_msg,
                code    =    err_code,
                state   =    err_sqlstate
            }

            print(require('json').encode(objDt))

            io.flush()
        end
    end
end

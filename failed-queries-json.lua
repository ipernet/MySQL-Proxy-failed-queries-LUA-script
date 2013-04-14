-----------------------------------------------------------------------------
-- A LUA script for MySQL proxy to output failed SQL queries as JSON
--
-- Version: 0.1
-- This script is released under the MIT License (MIT).
-- Please see LICENCE.txt for details.
--
-- REQUIREMENTS:
--   json module (https://github.com/craigmj/json4lua)
-----------------------------------------------------------------------------

function read_query(packet)
    if packet:byte() == proxy.COM_QUERY then
        proxy.queries:append(1, packet, {resultset_is_needed = true})
        return proxy.PROXY_SEND_QUERY
    end
end

function read_query_result(inj)
    local res = assert(inj.resultset)
	
    if(res.query_status == proxy.MYSQLD_PACKET_ERR) or (res.warning_count > 0) then

        local query        =	string.sub(inj.query, 2)
        local err_code     =	res.raw:byte(2) + (res.raw:byte(3) * 256)
        local err_sqlstate =	res.raw:sub(5, 9)
        local err_msg      =	res.raw:sub(10)

        if(err_code ~= 0) and (err_code ~= 1) then
            
			local objDt		=	{
				user	=	proxy.connection.server.username,
				db		=	proxy.connection.server.default_db,
				query	=	query,
				msg		=	err_msg,
				code	=	err_code,
				state	=	err_sqlstate
			}

			print(require('json').encode(objDt));
			
            io.flush()
        end
    end
end
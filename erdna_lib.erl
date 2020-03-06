-module(erdna_lib).


%% Strait interface to sedna API
-export([segetlasterrormsg/1,
         segetlasterrorcode/1,
         seconnect/6,
         seclose/1,
         sesetconnectionattr/3,
         segetconnectionattr/2,
         seresetallconnectionattr/1,
         sebegin/1,
         secommit/1,
         serollback/1,
         seconnectionstatus/1,
         setransactionstatus/1,
         seexecute/2,
         seexecutelong/2,
         senext/1,
         segetdata/1,
         sesetdebughandler/2,
         seloaddata/3,
         seloaddata/4,
         seendloaddata/1]).

%% Convenience function
-export([seconnect/4,
         segetnextdata/1,
         segetalldata/1,
         segetparsealldata/1]).

%% @doc Get code an text description of last error.
%% @param Port Erlang port to running erdna process.
%% @returns {ok,<<Description_of_Sedna's_error>>} or {error,
%% <<Description_of_erdna's_error>>}
segetlasterrormsg(Port) ->
    Data=erlang:term_to_binary({segetlasterrormsg}),
    run_command(Port,Data).

%% @doc Get Sedna's internal code of last error.
%% @param Port Erlang port to running erdna process.
%% @returns {ok, <<Decimal_Number>>} or {error,<<Description>>}, where
%% Description is a concise explanation of what went wrong.
segetlasterrorcode(Port) ->
    Data=erlang:term_to_binary({segetlasterrorcode}),
    run_command(Port,Data).

%% @doc Establishes connection to Sedna server with default login and password.
%% This should normally be the first step in any workflow.
%% @param Path Path to erdna executable, a binary (e.g. <<"/path/to/erdna">>).
%% @param Logging Whether to write a log file, 'true' of 'false'.
%% @param ServerURL URL of Sedna server - hostname or IP address optionally
%%                  followed by port number, a binary (e.g. <<"srv01:5050">>).
%% @param DBName Name of database, a binary.
%% @returns {ok,Port} on success or {error,<<Description>>} on failure.
seconnect(Path, Logging, ServerURL, DBName) ->
    seconnect(Path, Logging, ServerURL, DBName, <<"SYSTEM">>, <<"MANAGER">>).

%% @doc Establishes connection to Sedna server with default login and password.
%% This should normally be the first step in any workflow.
%% @param Path Path to erdna executable, a binary (e.g. <<"/path/to/erdna">>).
%% @param Logging Whether to write a log file, 'true' of 'false'.
%% @param ServerURL URL of Sedna server - hostname or IP address optionally
%% followed by port number, a binary (e.g. <<"srv01.dmz.local:5050">>).
%% @param DBName Name of database, a binary.
%% @returns {ok,Port} on success or {error,<<Description>>} on failure.
seconnect(Path, Logging, ServerURL, DBName, Login, Password) ->
    WorkDir=filename:dirname(Path),
    Options=case Logging of
             true ->
              [{packet,4},{cd,WorkDir},{args,[<<"-log">>]},binary];
             false ->
              [{packet,4},{cd,WorkDir},binary]
            end,
    Port=erlang:open_port({spawn_executable,Path},Options),
    Data=erlang:term_to_binary({seconnect, ServerURL, DBName, Login, Password}),
    case run_command(Port,Data) of
        {ok, _} -> {ok, Port};
        Error -> Error
    end.

%% @doc Makes erdna to close connection to Sedna and exit.
%% @param Port Erlang port to running erdna process.
%% @returns Unconditionally 'true'.
seclose(Port) ->
    Data=erlang:term_to_binary({seclose}),
    run_command(Port,Data),
    erlang:port_close(Port).

%% @doc Set value for connection parameter ("attribute"). For the list of
%% possible attributes and their values see Sedna Programmer's Guide.
%% @param Port Erlang port to running erdna process.
%% @param AttrName Attribute name, a binary.
%% @param AttrValue Value to be assigned? a binary.
%% @returns {ok,<<"Attr set succeeded">>} on success or {error,<<Description>>}
%% on failure.
sesetconnectionattr(Port, AttrName, AttrValue) -> 
    Data=erlang:term_to_binary({sesetconnectionattr, AttrName, AttrValue}),
    erlang:port_command(Port,Data),
    Result=receive
            {Port,{data,Bin}}->
             erlang:binary_to_term(Bin);
            _ ->
             {error, unknown}
            after 60000 ->
             erlang:port_close(Port),
             {error,timeout}
           end,
    Result.

%% @doc Get current value of connection attribute (except few ones - see
%% readme.txt).
%% @param Port Erlang port to running erdna process.
%% @param AttrName Name of the attribute of interest, a binary.
%% @returns {ok,<<Value>>} on success or {error,<<Description>>} on failure.
segetconnectionattr(Port, AttrName) -> 
    Data=erlang:term_to_binary({segetconnectionattr, AttrName}),
    run_command(Port,Data).

%% @doc Set all attributes for current session to their default values.
%% @param Port Erlang port to running erdna process.
%% @returns {ok,<<"Attrs reset">>} on success or {error,<<Description>>}
%% otherwise.
seresetallconnectionattr(Port) ->
    Data=erlang:term_to_binary({seresetallconnectionattr}),
    run_command(Port,Data).

%% @doc Manually begin transaction (when autocommit is disabled).
%% @param Port Erlang port to running erdna process.
%% @returns {ok,<<"Begin transaction succeeded">>} on success or {error,
%% <<Description>>} on failure.
sebegin(Port) ->
    Data=erlang:term_to_binary({sebegin}),
    run_command(Port,Data).

%% @doc Manually commits currently started transaction, pending changes will
%% be applied.
%% @param Port Erlang port to running erdna process.
%% @returns {ok,<<"Commit transaction succeeded">> on success or {error,
%% <<Description>>} on failure.
secommit(Port) ->
    Data=erlang:term_to_binary({secommit}),
    run_command(Port,Data).

%% @doc Manually rolls back currently started transaction, pending changes will
%% be lost.
%% @param Port Erlang port to running erdna process.
%% @returns {ok,<<"Rollback transaction succeeded">>} on success or {error,
%% <<Description>>} on failure.
serollback(Port) ->
    Data=erlang:term_to_binary({serollback}),
    run_command(Port,Data).

%% @doc Check connection to Sedna server. This function should not be relied
%% upon (see readme.txt for details).
%% @param Port Erlang port to running erdna process.
%% @returns {ok,<<"Connection open">>} or {ok,<<"Connection closed">>}
%% when executed successully, or {error,<<Description>>} on failure.
seconnectionstatus(Port) ->
    Data=erlang:term_to_binary({seconnectionstatus}),
    run_command(Port,Data).

%% @doc Informs whether a transaction has been started and not yet commeted.
%% @param Port Erlang port to running erdna process.
%% @returns {ok,<<"Transaction is running">>} or {ok,
%% <<"No transaction running">>} on success, {error,<<Description>>} on
%% failure.
setransactionstatus(Port) ->
    Data=erlang:term_to_binary({setransactionstatus}),
    run_command(Port,Data).

%% @doc Send a query to Sedna server for execution.
%% @param Port Erlang port to running erdna process.
%% @param Query XQuery statement to be executed, a binary.
%% @returns either {ok,<<"Query succeeded">>} or {ok,<<"Update succeeded">>}
%% or {ok,<<"Bulk load succeeded">> on success, {error,<<Description>>}
%% otherwise. Query result is not returned and is to be retrieved separately.
seexecute(Port,Query) ->
    Data=erlang:term_to_binary({seexecute,Query}),
    run_command(Port,Data).

%% @doc Execute an XQuery script stored in a file.
%% @param Port Erlang port to running erdna process.
%% @param Path Full path to XQuery script file, a binary (or relative path
%% when SEDNA_ATTR_SESSION_DIRECTORY is set).
%% @returns {ok,<<"Query succeeded">>} or {ok,<<"Update succeeded">>}
%% or {ok,<<"Bulk load succeeded">> on success, {error,<<Description>>}
%% otherwise. Query result is not returned and is to be retrieved separately.
seexecutelong(Port, Path) ->
    Data=erlang:term_to_binary({seexecutelong,Path}),
    run_command(Port,Data).

%% @doc Select an item from the query result waiting for retrieval. Must be
%% executed before requesting the item.
%% @param Port Erlang port to running erdna process.
%% @returns {ok,<<"Next succeeded">>} on success, {error,<<Description>>}
%% otherwise.
senext(Port) ->
    Data=erlang:term_to_binary({senext}),
    run_command(Port,Data).

%% @doc Get one item from the query result.
%% @param Port Erlang port to running erdna process.
%% @returns {ok,<<Data>>} on success, {error,<<Description>>} otherwise.
segetdata(Port) ->
    Data=erlang:term_to_binary({segetdata}),
    run_command(Port,Data).

%% @doc Set handler for Sedna debug data (handler is a pointer to a function
%% provided in erdna).
%% @param Port Erlang port to running erdna process.
%% @param Type Name of handler, a binary. Currently can only be LOG or NONE.
%% @returns {ok,<<"Debug logging started">>} or {ok,
%% <<"Debug logging stopped">>} on success, {error,<<Description>>} on failure.
sesetdebughandler(Port,Type) ->
    Data=erlang:term_to_binary({sesetdebughandler,Type}),
    run_command(Port,Data).

%% @doc Load piece of XML data to Sedna document. Sequence of pieces must
%% form a valid XML document.
%% @param Port Erlang port to running erdna process.
%% @param DataChunk - fragment of document, a binary.
%% @param Document - target document name, a binary.
%% @returns {ok,<<"Data loaded">>} on success or {error,<<Description>>} on
%% failure.
seloaddata(Port, DataChunk, Document) ->
    Data=erlang:term_to_binary({seloaddata, DataChunk, Document}),
    run_command(Port,Data).

%% @doc Load piece of XML data to Sedna document in specified collection.
%% Sequence of pieces must form a valid XML document.
%% @param Port Erlang port to running erdna process.
%% @param DataChunk - fragment of document, a binary.
%% @param Document - target document name, a binary.
%% @param Collection - collection to which target document belongs.
%% @returns {ok,<<"Data loaded">>}
seloaddata(Port, DataChunk, Document, Collection) ->
    Data=erlang:term_to_binary({seloaddata, DataChunk, Document, Collection}),
    run_command(Port,Data).

%% @doc Indicate end of data just loaded and make Sedna to proceed to create
%% the document and place the data in it.
%% @param Port Erlang port to running erdna process.
%% @returns {ok,<<"Load finished">>} on success or {error,<<Description>>} on
%% failure.
seendloaddata(Port) ->
    Data=erlang:term_to_binary({seendloaddata}),
    run_command(Port,Data).

%% Convenience functions

%% @doc Set pointer to next item in query result and retrieve it from the
%% server.
%% @param Port Erlang port to running erdna process.
%% @returns {ok,<<Data>>} on success, {error,<<Description>>} on failure.
segetnextdata(Port) ->
    case senext(Port) of
        {ok,_} -> segetdata(Port);
        Error -> Error
    end.

%% @doc Get all items of query result currently waiting to be retrieved.
%% @param Port Erlang port to running erdna process.
%% @returns [<<Data1>>,<<Data2>>,...] on success (list can be empty), or
%% {error,<<Description>>} on failure.
segetalldata(Port) ->
    segetalldata(Port,[]).

%% @doc Get items of query result  currently waiting to be retrieved and
%% parse them usung exomler.
%% @param Port Erlang port to running erdna process.
%% @returns [Tuple,Tuple,...] on success or {error,<<Description>>}
%% on failure.
segetparsealldata(Port) ->
    segetparsealldata(Port,[]).


%% internals
%% @doc Actually performs all tasks, implemented in functions above, by sending
%% command and, optionally, data to running erdna process.
%% @param Port Erlang port to running erdna process.
%% @param Data Binary-encoded tuple, composed of command (an atom) and optional
%% data (a binary).
%% @returns Data received from erdna via port.
run_command(Port,Data) ->
    erlang:port_command(Port,Data),
    Result=receive
               {Port,{data,Bin}} ->
                   erlang:binary_to_term(Bin);
               _ ->
                   {error, unknown}
               % At present timeput is hardcoded.
               % ToDo: Move it to .hrl file or make configurable
                after 60000 ->
                   erlang:port_close(Port),
                   {error,timeout}
           end,
    Result.

%% @doc Get next query result item from Sedna and store it into accumulator,
%% then re-invoke itself; return contents of accumulator when no items left.
%% @param Port Erlang port to running erdna process.
%% @param Accum List of items retrieved from Sedna.
%% @returns [<<Data1>>,<<Data2>>,...] on success (list can be empty), or
%% {error,<<Description>>} on failure.
segetalldata(Port,Accum) ->
%% Retrieves all data waiting for retrieval after seexecute/2.
%% This function is supposed to be executed just after seexecute/2, so
%% Sedna's internal "pointer" is expected to be set on the first record.
    case senext(Port) of
        {ok,_} ->
            case segetdata(Port) of
                {ok,Data} ->
                     NewAccum=[Data|Accum],
                     segetalldata(Port,NewAccum);
                Error -> Error
            end;
        {error,<<"End reached">>} ->
            lists:reverse(Accum);
        {error,<<"No item">>} -> [];
        Error -> Error
    end.

%% @doc Consequently get all query result items from Sedna, parse them and
%% store into accumulator.
%% @param Port Erlang port to running erdna process.
%% @param Accum List of retrieved and parsed items.
%% @returns [Tuple,Tuple,...] on success or {error,<<Description>>} in case of
%% failur.
segetparsealldata(Port,Accum) ->
%% Retrieves all data waiting for retrieval after seexecute/2 and
%% immediately parses each record just upon its retrieval. This eliminates
%% need for storing retrieved records as binaries in the intermediate list,
%% thus redicing memory consumtion.
%% This function is supposed to be executed just after seexecute/2, so
%% Sedna's internal "pointer" is expected to be set on the first record.
    case senext(Port) of
        {ok,_} ->
            case segetdata(Port) of
                {ok,Data} ->
                     NewAccum=[exomler:decode(Data)|Accum],
                     segetparsealldata(Port,NewAccum);
                Error -> Error
            end;
        _ -> lists:reverse(Accum)
    end.


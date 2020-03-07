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

%% @doc Get code and text description of last error.
%% @param Port Erlang port to running erdna process.
%% @returns {ok,&lt;&lt;Description_of_Sedna's_error&gt;&gt;} or {error,
%%  &lt;&lt;Description_of_erdna's_error&gt;&gt;}.
-spec segetlasterrormsg(port())
      -> {ok,binary()} | {error,binary()}.
segetlasterrormsg(Port) ->
    Data=erlang:term_to_binary({segetlasterrormsg}),
    run_command(Port,Data).

%% @doc Get Sedna's internal code of last error.
%% @param Port Erlang port to running erdna process.
%% @returns {ok, &lt;&lt;Decimal_Number&gt;&gt;)} or {error,
%%  &lt;&lt;Description&gt;&gt;}, where Description is a concise explanation
%%  of what went wrong.
-spec segetlasterrorcode(port())
      -> {ok,binary()} | {error,binary()}.
segetlasterrorcode(Port) ->
    Data=erlang:term_to_binary({segetlasterrorcode}),
    run_command(Port,Data).

%% @doc Establishes connection to Sedna server with default login and password.
%%  This should normally be the first step in any workflow.
%% @param Path Path to erdna executable, a binary (e.g.
%%  &lt;&lt;"/path/to/erdna"&gt;&gt;).
%% @param Logging Whether to write a log file, 'true' of 'false'.
%% @param ServerURL URL of Sedna server - hostname or IP address optionally
%%  followed by port number, a binary (e.g. &lt;&lt;"srv01:5050"&gt;&gt;).
%% @param DBName Name of database, a binary.
%% @returns {ok,Port} on success or {error,&lt;&lt;Description&gt;&gt;} on
%%  failure.
-spec seconnect(binary(),boolean(),binary(),binary())
      -> {ok,port()} | {error,binary}.
seconnect(Path, Logging, ServerURL, DBName) ->
    seconnect(Path, Logging, ServerURL, DBName, <<"SYSTEM">>, <<"MANAGER">>).

%% @doc Establishes connection to Sedna server with specified login and
%%  password. This should normally be the first step in any workflow.
%% @param Path Path to erdna executable, a binary (e.g.
%%  &lt;&lt;"/path/to/erdna"&gt;&gt;).
%% @param Logging Whether to write a log file, 'true' of 'false'.
%% @param ServerURL URL of Sedna server - hostname or IP address optionally
%%  followed by port number, a binary (e.g.
%%  &lt;&lt;"srv01.servers.local:5050"&gt;&gt;).
%% @param DBName Name of database, a binary.
%% @returns {ok,Port} on success or {error,&lt;&lt;Description&gt;&gt;} on
%%  failure.
-spec seconnect(binary(),boolean(),binary(),binary(),binary(),binary())
      -> {ok,port()} | {error,binary()}.
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
-spec seclose(port()) -> true.
seclose(Port) ->
    Data=erlang:term_to_binary({seclose}),
    run_command(Port,Data),
    erlang:port_close(Port).

%% @doc Set value for connection parameter ("attribute"). For the list of
%%  possible attributes and their values see Sedna Programmer's Guide.
%% @param Port Erlang port to running erdna process.
%% @param AttrName Attribute name, a binary.
%% @param AttrValue Value to be assigned, a binary.
%% @returns {ok,&lt;&lt;"Attr set succeeded"&gt;&gt;} on success or {error,
%%  &lt;&lt;Description&gt;&gt;} on failure.
-spec sesetconnectionattr(port(),binary(),binary())
      -> {ok,binary()} | {error,binary()}.
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
%%  readme.txt).
%% @param Port Erlang port to running erdna process.
%% @param AttrName Name of the attribute of interest, a binary.
%% @returns {ok,&lt;&lt;Value&gt;&gt;} on success or {error,
%%  &lt;&lt;Description&gt;&gt;}
%% on failure.
-spec segetconnectionattr(port(),binary())
      -> {ok,binary()} | {error,binary()}.
segetconnectionattr(Port, AttrName) -> 
    Data=erlang:term_to_binary({segetconnectionattr, AttrName}),
    run_command(Port,Data).

%% @doc Set all attributes for current session to their default values.
%% @param Port Erlang port to running erdna process.
%% @returns {ok,&lt;&lt;"Attrs reset"&gt;&gt;} on success or {error,
%%  &lt;&lt;Description&gt;&gt;} otherwise.
-spec seresetallconnectionattr(port())
      -> {ok,binary()} | {error,binary()}.
seresetallconnectionattr(Port) ->
    Data=erlang:term_to_binary({seresetallconnectionattr}),
    run_command(Port,Data).

%% @doc Manually begin transaction (when autocommit is disabled).
%% @param Port Erlang port to running erdna process.
%% @returns {ok,&lt;&lt;"Begin transaction succeeded"&gt;&gt;} on success or
%%  {error,&lt;&lt;Description&gt;&gt;} on failure.
-spec sebegin(port())
      -> {ok,binary()} | {error,binary()}.
sebegin(Port) ->
    Data=erlang:term_to_binary({sebegin}),
    run_command(Port,Data).

%% @doc Manually commits currently started transaction, pending changes will
%%  be applied.
%% @param Port Erlang port to running erdna process.
%% @returns {ok,&lt;&lt;"Commit transaction succeeded"&gt;&gt; on success or
%%  {error,&lt;&lt;Description&gt;&gt;} on failure.
-spec secommit(port())
      -> {ok,binary()} | {error,binary()}.
secommit(Port) ->
    Data=erlang:term_to_binary({secommit}),
    run_command(Port,Data).

%% @doc Manually rolls back currently started transaction, pending changes will
%%  be lost.
%% @param Port Erlang port to running erdna process.
%% @returns {ok,&lt;&lt;"Rollback transaction succeeded"&gt;&gt;} on success or {error,
%% &lt;&lt;Description&gt;&gt;} on failure.
-spec serollback(port())
      -> {ok,binary()} | {error,binary()}.
serollback(Port) ->
    Data=erlang:term_to_binary({serollback}),
    run_command(Port,Data).

%% @doc Check connection to Sedna server. This function should not be relied
%%  upon (see readme.txt for details).
%% @param Port Erlang port to running erdna process.
%% @returns {ok,&lt;&lt;"Connection open"&gt;&gt;} or {ok,
%%  &lt;&lt;"Connection closed"&gt;&gt;} when executed successully, or
%%  {error,&lt;&lt;Description&gt;&gt;} on failure.
-spec seconnectionstatus(port())
      -> {ok,binary()} | {error,binary()}.
seconnectionstatus(Port) ->
    Data=erlang:term_to_binary({seconnectionstatus}),
    run_command(Port,Data).

%% @doc Informs whether a transaction has been started and not yet commeted.
%% @param Port Erlang port to running erdna process.
%% @returns {ok,&lt;&lt;"Transaction is running"&gt;&gt;} or {ok,
%%  &lt;&lt;"No transaction running"&gt;&gt;} on success, {error,
%%  &lt;&lt;Description&gt;&gt;} on failure.
-spec setransactionstatus(port())
      -> {ok,binary()} | {error,binary()}.
setransactionstatus(Port) ->
    Data=erlang:term_to_binary({setransactionstatus}),
    run_command(Port,Data).

%% @doc Send a query to Sedna server for execution.
%% @param Port Erlang port to running erdna process.
%% @param Query XQuery statement to be executed, a binary.
%% @returns either {ok,&lt;&lt;"Query succeeded"&gt;&gt;} or {ok,
%%  &lt;&lt;"Update succeeded"&gt;&gt;} or {ok,
%%  &lt;&lt;"Bulk load succeeded"&gt;&gt; on success, {error,
%%  &lt;&lt;Description&gt;&gt;} otherwise. Query result is not returned
%%  and is to be retrieved separately.
-spec seexecute(port(),binary())
      -> {ok,binary()} | {error,binary()}.
seexecute(Port,Query) ->
    Data=erlang:term_to_binary({seexecute,Query}),
    run_command(Port,Data).

%% @doc Execute an XQuery script stored in a file.
%% @param Port Erlang port to running erdna process.
%% @param Path Full path to XQuery script file, a binary (or relative path
%%  when SEDNA_ATTR_SESSION_DIRECTORY is set).
%% @returns {ok,&lt;&lt;"Query succeeded"&gt;&gt;} or {ok,
%%  &lt;&lt;"Update succeeded"&gt;&gt;} or {ok,
%%  &lt;&lt;"Bulk load succeeded"&gt;&gt; on success, {error,
%%  &lt;&lt;Description&gt;&gt;} otherwise. Query result is not returned
%%  and is to be retrieved separately.
-spec seexecutelong(port(),binary())
      -> {ok,binary()} | {error,binary()}.
seexecutelong(Port, Path) ->
    Data=erlang:term_to_binary({seexecutelong,Path}),
    run_command(Port,Data).

%% @doc Select an item from the query result waiting for retrieval. Must be
%%  executed before requesting the item.
%% @param Port Erlang port to running erdna process.
%% @returns {ok,&lt;&lt;"Next succeeded"&gt;&gt;} on success, {error,
%%  &lt;&lt;Description&gt;&gt;} otherwise.
senext(Port) ->
    Data=erlang:term_to_binary({senext}),
    run_command(Port,Data).

%% @doc Get one item from the query result.
%% @param Port Erlang port to running erdna process.
%% @returns {ok,&lt;&lt;Data&gt;&gt;} on success, {error,
%% &lt;&lt;Description&gt;&gt;} otherwise.
-spec segetdata(port())
      -> {ok,binary()} | {error,binary()}.
segetdata(Port) ->
    Data=erlang:term_to_binary({segetdata}),
    run_command(Port,Data).

%% @doc Set handler for Sedna debug data (handler is a pointer to a function
%%  provided in erdna).
%% @param Port Erlang port to running erdna process.
%% @param Type Name of handler, a binary. Currently can only be LOG or NONE.
%% @returns {ok,&lt;&lt;"Debug logging started">>} or {ok,
%%  &lt;&lt;"Debug logging stopped">>} on success or {error,
%%  &lt;&lt;Description&gt;&gt;} on failure.
-spec sesetdebughandler(port(),binary())
      -> {ok,binary()} | {error,binary()}.
sesetdebughandler(Port,Type) ->
    Data=erlang:term_to_binary({sesetdebughandler,Type}),
    run_command(Port,Data).

%% @doc Load piece of XML data to Sedna document. Sequence of pieces must
%%  form a valid XML document.
%% @param Port Erlang port to running erdna process.
%% @param DataChunk - fragment of document, a binary.
%% @param Document - target document name, a binary.
%% @returns {ok,&lt;&lt;"Data loaded"&gt;&gt;} on success or {error,
%%  &lt;&lt;Description&gt;&gt;} on failure.
-spec seloaddata(port(),binary(),binary())
      -> {ok,binary()} | {error,binary()}.
seloaddata(Port, DataChunk, Document) ->
    Data=erlang:term_to_binary({seloaddata, DataChunk, Document}),
    run_command(Port,Data).

%% @doc Load piece of XML data to Sedna document in specified collection.
%%  Sequence of pieces must form a valid XML document.
%% @param Port Erlang port to running erdna process.
%% @param DataChunk - fragment of document, a binary.
%% @param Document - target document name, a binary.
%% @param Collection - collection to which target document belongs.
%% @returns {ok,&lt;&lt;"Data loaded"&gt;&gt;}
-spec seloaddata(port(),binary(),binary(),binary())
      -> {ok,binary()} | {error,binary()}.
seloaddata(Port, DataChunk, Document, Collection) ->
    Data=erlang:term_to_binary({seloaddata, DataChunk, Document, Collection}),
    run_command(Port,Data).

%% @doc Indicate end of data just loaded and make Sedna to proceed to create
%%  the document and place the data in it.
%% @param Port Erlang port to running erdna process.
%% @returns {ok,&lt;&lt;"Load finished"&gt;&gt;} on success or {error,
%%  &lt;&lt;Description&gt;&gt;} on failure.
-spec seendloaddata(port())
      -> {ok,binary()} | {error,binary()}.
seendloaddata(Port) ->
    Data=erlang:term_to_binary({seendloaddata}),
    run_command(Port,Data).

%% Convenience functions

%% @doc Set pointer to next item in query result and retrieve it from the
%% server.
%% @param Port Erlang port to running erdna process.
%% @returns {ok,&lt;&lt;Data&gt;&gt;} on success, {error,
%%  &lt;&lt;Description&gt;&gt;} on failure.
-spec segetnextdata(port())
      -> {ok,binary()} | {error,binary()}.
segetnextdata(Port) ->
    case senext(Port) of
        {ok,_} -> segetdata(Port);
        Error -> Error
    end.

%% @doc Get all items of query result currently waiting to be retrieved.
%% @param Port Erlang port to running erdna process.
%% @returns [&lt;&lt;Data1&gt;&gt;,&lt;&lt;Data2&gt;&gt;,...] on success
%%  (list can be empty), or {error,&lt;&lt;Description&gt;&gt;} on failure.
-spec segetalldata(port())
      -> [binary()].
segetalldata(Port) ->
    segetalldata(Port,[]).

%% @doc Get items of query result  currently waiting to be retrieved and
%% parse them usung exomler.
%% @param Port Erlang port to running erdna process.
%% @returns [Tuple,Tuple,...] on success or {error,&lt;&lt;Description&gt;&gt;}
%% on failure.
-spec segetparsealldata(port())
      -> [tuple()].
segetparsealldata(Port) ->
    segetparsealldata(Port,[]).


%% Internals

%% @doc Actually performs all tasks, implemented in functions above, by sending
%% command and, optionally, data to running erdna process.
%% @param Port Erlang port to running erdna process.
%% @param Data Binary-encoded tuple, composed of command (an atom) and optional
%% data (a binary).
%% @returns Data received from erdna via port.
-spec run_command(port(),binary())
      -> {ok,binary()} | {error,binary()}.
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
%%  then re-invoke itself; return contents of accumulator when no items left.
%% @param Port Erlang port to running erdna process.
%% @param Accum List of items retrieved from Sedna.
%% @returns [&lt;&lt;Data1&gt;&gt;,&lt;&lt;Data2&gt;&gt;,...] on success (list
%%  can be empty), or{error,&lt;&lt;Description&gt;&gt;} on failure.
-spec segetalldata(port(),[binary()])
      -> [binary()].
segetalldata(Port,Accum) ->
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
%% @returns [Tuple,Tuple,...] on success or {error,&lt;&lt;Description&gt;&gt;}
%%  in case offailure.
-spec segetparsealldata(port(),[tuple()])
      -> [tuple()]
segetparsealldata(Port,Accum) ->
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


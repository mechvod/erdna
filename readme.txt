Erdna is a simple intermediary between Erlang VM and Sedna XML database.
Data interchange with EVM is going through Erlang port (i.e. through STDIN/
STDOUT for erdna's viewpoint). Data interchange with Sedna is going through
TCP connection (usually via port 5050).
Technically, Erdna is just a wrapper around Sedna's C functions (see section
"C API" in Sedna Programmer's Guide).
To ease using erdna, there is a set of convenience functions gathered into
erlang module erdna_lib.
Brief description of functions in erdna_lib:
seconnect(Path,Logging,ServerURL,DBName,Login,Password) - open connection
 to Sedna database. On success returns {ok,Port}, on error - either complains
 about bad port, or (if erdna has started but failed to connect to server)
 transparently passes returned value from erdna.
 Arguments are:
 Path - full path to erdna executable, a binary, (e.g. <<"/home/user/erdna">>);
 Logging - whether erdna will record its activity to a log file. Must be
           atom 'true' or 'false'. Logfile will be created in the same
           directory where erdna resides and will be named "erdnalog_PID_XXXXXX"
           where PID will be PID of erdna process and XXXXXX is an arbitrary
           string (see mkstemp(3));
 ServerURL - Sedna server URL, a binary (e.g. <<"localhost:5050">>);
 DBName - name of XML database to connect to, a binary (e.g. <<"fias">>);
 Login - name of Sedna's user, a binary (e.g. <<"USER01">>);
 Password - password for that user, a binary (e.g. <<"VeryLongAndComplex#1">>);
seconnect(Path,Logging,ServerURL,DBName) - the same as seconnect/6 but
 default username <<"SYSTEM">> with password <<"MANAGER">> are added. This
 might be convenient for development, but avoid using it in production
 environment.
seconnectionstatus(Port) - check status of connection between erdna process
 (the client) and Sedna server. Port is erlang port to running erdna process.
 On successful execution returns {ok,<<"Connection open">>} or {ok,
 <<"Connection closed">>}. Don't rely on this function - if you stop Sedna
 storage manager by issuing "./se_smsd yourdatabasename" at your OS shell
 prompt, this function still reports {ok,<<"Connection open">>}. Some other
 functions (those that rely on data stored at client-side, e.g.
 segetconnectionattr/2), succesfully return even after errors encountered
 by other functions (those that depend on working connection).
sesetconnectionattr(Port, AttrName, AttrValue) - set the attribute to the
 specified value for the session indicated by Port.
 Arguments are:
 Port - erlang port to erdna process with open connection to Sedna DB;
 AttrName - name of the attribute, a binary (e.g. <<"SEDNA_ATTR_AUTOCOMMIT">>);
 AttrValue - value for the attribute, a binary (e.g. <<"ON">>). All values are
 passed as binaries, including booleans (i.e. <<"ON">> or <<"OFF">> for
 SEDNA_ATTR_AUTOCOMMIT above) and integers (e.g. <<"60">> for
 SEDNA_ATTR_QUERY_EXEC_TIMEOUT to set it to one minute).
 Following is the list of all session attributes and their possible values:
  SEDNA_ATTR_AUTOCOMMIT - <<"ON">> or <<"OFF">>;
  SEDNA_ATTR_DEBUG - <<"ON">> or <<"OFF">>;
  SEDNA_ATTR_BOUNDARY_SPACE_PRESERVE_WHILE_LOAD - <<"ON">> or <<"OFF">>;
  SEDNA_ATTR_CDATA_PRESERVE_WHILE_LOAD - <<"ON">> or <<"OFF">>;
  SEDNA_ATTR_SESSION_DIRECTORY - <<"/path/to/directory">>;
  SEDNA_ATTR_CONCURRENCY_TYPE - <<"READONLY">> or <<"UPDATE">>;
  SEDNA_ATTR_LOG_AMOUNT - <<"FULL">> or <<"LESS">>
  SEDNA_ATTR_QUERY_EXEC_TIMEOUT - binary representing number of seconds
                                  (decimal integer value, e.g. <<"60">> 
                                  for 1 minute, <<"0">> for infinite timeout);
  SEDNA_ATTR_MAX_RESULT_SIZE - binary representing number of bytes (decimal
                               integer value, <<"0">> for no limit).
 See section "Setting Session Options" in Sedna Programmer’s Guide for details
 about names of attributes and their values (however, be aware that there are
 two attributes, SEDNA_ATTR_BOUNDARY_SPACE_PRESERVE_WHILE_LOAD and
 SEDNA_ATTR_CDATA_PRESERVE_WHILE_LOAD, not mentioned in Programmer's Guide,
 so I am not sure if you should to mess with them);
segetconnectionattr(Port, AttrName) - get current value of session attribute
 Arguments are:
 Port - erlang port to erdna process with open connection to Sedna DB;
 AttrName - name of the attribute. Attribute can be the same as for
 sesetconnectionattr/3 with some exceptions - SEDNA_ATTR_DEBUG,
 SEDNA_ATTR_CONCURRENCY_TYPE and SEDNA_ATTR_LOG_AMOUNT are not supported
 by SEgetConnectionAttr (only attributes kept in struct SednaConnection on
 client side can be obtained by this function, while these are set on
 server side).
seresetallconnectionattr(Port) - reset all connection attributes to their
 default values. Port is erlang port to erdna process with open connection
 to Sedna DB.
seexecute(Port,Query - send query to Sedna DB. Arguments are:
 Port - erlang port to erdna process;
 Query - query to Sedna DB, a binary. Must be valid XQuery 1.0 statement, see
 https://w3.org/ for details. Example: <<"fn:doc(\"universe\")/Characters/\
 Character[(@SKILL eq \"archer\") and (@RACE eq \"elf\")]/(fn:data(@NAME))">>).
 On success returns just {ok,<<"Query succeeded">>}, use senext/1 and
 segetdata/1 to get the result of the query (see below).
senext(Port) - shifts Sedna's internal pointer to the next line of output
 ("item") waiting for be retrieved. On success returns {ok,
 <<"Next succeeded">>} (in that case selected line can be retrieved from
 Sedna), otherwise returns {error,<<"End reached">>} or {error,<<"No item">>}
 (in that case attempt to retrieve a line will result in {ok,<<>>}).
 Note that senext/1 should be used right after executing the query before
 requesting the first line with segetdata/1.
segetdata(Port) - fetch the result of previously executed query. Port is
 erlang port to running sedna process. On success returns a tuple with one
 line of query result currently awaiting for retrieval (e.g.
 {ok,<<"<Character Name=\"Gimli\" Race=\"dwarf\" Skill=\"axeman\">\n
   <Description>Gimli is a noble dwarf wielding heavy do"...>>}
 Here the string is broken into two lines for readability, in practice it is
 returned as a single line, and note the escaped newline and indenting
 spaces before the Description section. They come from the source XML file
 and you should be prepared to deal with them. Hopefully, exomler (XML parser
 that I prefer) works well with that.
segetnextdata(Port) - convenience function that retrieves the next item from
 query result. It is just senext/1 and segetdata/1 coupled together.
segetalldata(Port) - convenience function that retrieves the whole result of
 query as a list of binaries by repeatedly invoking senext/1 and segetdata/1.
segetparsealldata(Port) - does the same as segetalldata/1 but every record is
 parsed by exomler:decode/1, thus the result is a list of terms each being the
 result of parsing one item. Obviously, exomler by SCDependencies needs to
 be instaled on your host.
seexecutelong(Port,Path) - execute XQery statements stored in a file.
 Makes it possible to run long and complex scripts. Port is an erlang port
 to running erdna process, Path is a path to a file with XQuery script. Result
 of execution of the script can be obtained by the same means as for
 seexecute/2 (i.e. senext/1, segetdata/1, segetnextdata/1, segetalldata/1,
 segetparsealldata/1).
seloaddata(Port,Data,Document) - send a portion of XML document.
 Used to create new XML document in current database by sending to Sedna a
 sequence of fragments (chunks) of the document. Document will not be actually
 created untill the end of data is indicated by seendloaddata/1.
seloaddata(Port,Data,Document,Collection) - the same as seloaddata/3 but
 the document will be created in the specified collection.
seendloaddata(Port) - notifies Sedna that the whole set of data is transferred
 and it's time to actually create the document and put the data in it.
 This must immediately follow the last seloaddata/3,4 in the sequence.
segetlasterrormsg(Port) - get brief description and code of recent error
 occured in current session. Port is erlang port to running erdna process.
 For example, attempt to get value of connection attribute SEDNA_ATTR_DEBUG
 will result in <<"Unspecified error">> and subsequent invocation of
 segetlasterrormsg/1 will return {ok,<<"SEDNA Message: ERROR SE3022\nInvalid \
 argument.">>}
segetlasterrorcode(Port) - get numeric code of latest error in current session.
 Port is erlang port to running erdna process.
 For example, invoked after unsuccessful segetconnectionattr(Port,
 <<"SEDNA_ATTR_DEBUG">>) this function will return {ok,<<"243">>}.
sebegin(Port) - manually start new transaction (when SEDNA_ATTR_AUTOCOMMIT is
 set to OFF). All subsequent statements sent to Sedna by seexecute/2 up to
 secommit/1 will constitute a single transaction.
secommit(Port) - manually commit previously started transaction, all pending
 changes will be applied.
serollback(Port) - manually roll back previously started transaction. All
 pending changes will be lost.
 Port is erlang port to running erdna process.
setransactionstatus(Port) - checks if there is a started transaction within
 current session, waiting to be commited (or rolled back). Port is erlang port
 to running erdna process. On success returns {ok,<<"Transaction is running">>}
 or {ok,<<"No transaction running">>}.
sesetdebughandler(Port,Handler) - set a handler for Sedna's debug trace. Port
 is erlang port to erdna process, Handler is a handler name, a binary. At
 present the only supported options are <<"LOG">> (write to log file) and
 <<"NONE">> (stop logging if it was on, no-op if it was off).
seclose(Port) - close connection to Sedna and terminate erdna. Port becomes
 invalid.
Erdna is Unicode-ignorant. It blindly copies bytes (i.e. 8-bit pieces of data,
to eliminate ambiguity) without any care of whether they make correct UTF-8
characters or not. See corresponding section in examples.txt for details.
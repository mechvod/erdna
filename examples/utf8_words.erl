-module(utf8_words).

-export([make_page/4]).

make_page(ErdnaPath,SednaURL,DBName,OutFilePath) ->
 {ok,OutFile}=file:open(OutFilePath,[write]),
 DirName=filename:dirname(OutFilePath),
 QueryFilePath= <<DirName/binary,"/utf8_words_erdna_test.xq">>,

 % Write static part of html page
 file:write(OutFile,<<"<html>",13,10,"<head>",13,10,"<meta http-equiv=",
                            "\"Content-Type\" content=\"text/html; charset=",
                            "UTF-8\">",13,10,"<title>UTF-8 test page</title>",
                            13,10,"</head>",13,10,"<body>",13,10>>),

 {ok,Port}=erdna_lib:seconnect(ErdnaPath,false,SednaURL,DBName),

 % Get the list of section names (which are the meanings of the words).
 erdna_lib:seexecute(Port,<<"for $node in fn:doc(\"utf8_words\")/Words/*",
                            " return(fn:node-name($node))">>),
 SectionNames=erdna_lib:segetalldata(Port),
 make_lists(Port,OutFile,QueryFilePath,SectionNames),
 file:write(OutFile,<<"</body>",13,10,"</html>">>),
 file:close(OutFile),
 erdna_lib:seclose(Port).

 make_lists(_Port,_OutFile,_QueryFilePath,[]) -> true;
 make_lists(Port,OutFile,QueryFilePath,[SectionName|Rest]) ->
  file:write(OutFile,<<"<div>",13,10,SectionName/binary,"<br>",13,10,
                                "<ul>",13,10>>),
  % Sedna does not support external variables (Programmer's Guide explicitly
  % states that, section 2.1 "XQuery Support"), so we need to prepare
  % a file with XQuery script with section name hardcoded in it.
  {ok,QueryFile}=file:open(QueryFilePath,[write]),
  file:write(QueryFile,<<"for $wordnode in fn:doc(\"utf8_words\")/Words/",
                          SectionName/binary,"/Word",13,10,
                          " let $lang:=fn:data($wordnode/@Lang),",13,10,
                          "     $word:=fn:data($wordnode),",13,10,
                          "     $translate_link_part1:=\"https://translate.",
                          "google.com/#view=home&amp;op=translate&amp;sl=\",",
                          13,10,
                          "     $translate_link_part2:=\"&amp;tl=en&amp;",
                          "text=\",",13,10,
                          "    $translate_link:=fn:concat($translate_link",
                          "_part1,$lang,$translate_link_part2,$word)",13,10,
                          " return (",13,10,
                          "fn:concat(\"<li> \",$lang,\": \",$word,\" (<a href",
                          "=&quot;\",$translate_link,\"&quot;>translate</a>)",
                          "</li>\")",13,10,")">>),
  file:close(QueryFile),
  erdna_lib:seexecutelong(Port,QueryFilePath),
  print_lines(Port,OutFile),
  file:write(OutFile,<<"</ul>",13,10,"</div>",13,10>>),
  make_lists(Port,OutFile,QueryFilePath,Rest).

print_lines(Port,File) ->
    case erdna_lib:segetnextdata(Port) of
        {ok,Line} ->
            LineDecoded=decode_line(Line),
            file:write(File,<<LineDecoded/binary,13,10>>),
            print_lines(Port,File);
        _ ->
            true
    end.

decode_line(Line) ->
    Line1=binary:replace(Line,<<"&lt;">>,<<"<">>,[global]),
    Line2=binary:replace(Line1,<<"&gt;">>,<<">">>,[global]),
    Line3=binary:replace(Line2,<<"&amp;">>,<<"&">>,[global]),
          binary:replace(Line3,<<"&quot;">>,<<"\"">>,[global]).

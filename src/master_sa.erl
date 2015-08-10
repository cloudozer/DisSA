% distributed Suffix array construction
%
% 
% Cloudozer(c), 2015
%

-module(master_sa).
-export([
		t/0,
		main/2,
		merge_sa/2
		]).

-define(SORTED_LEN,150000).



t() -> 
	Tests = [
		"21"
		%"GL000207.1"
		%"GL000226.1",
		%"GL000229.1",
		%"GL000231.1",
		%"GL000239.1",
		%"GL000235.1",
		%"GL000201.1",
		%"GL000247.1",
		%"GL000245.1",
		%"GL000192.1",
		%"GL000193.1"
	],
	t(Tests).

t([Chromo|Tests]) ->
	statistics(wall_clock),
	File = "data/"++Chromo++".ref",
	Worker_nbr = 5,
	main(File,Worker_nbr),
	{_,T} = statistics(wall_clock),
	io:format("SA construction took ~p secs~n",[T/1000]),
	%{ok,Bin} = file:read_file(File),
	%io:format("Test passed: ~p~n",[DSA1==sa(binary_to_list(Bin))]),
	t(Tests);
t([]) -> ok.


main(File,Worker_nbr) ->
<<<<<<< HEAD:master.erl
	Size = filelib:file_size(File),
	case  Size < ?SORTED_LEN of
		true -> LogLen = 0;
		false-> LogLen = round(math:log(Size / ?SORTED_LEN) / math:log(4))
	end,
	
=======
	register(master, self()),

	Size = filelib:file_size(filename:join("priv", File)),
	LogLen = round(math:log(Size / ?SORTED_LEN) / math:log(4)),
>>>>>>> b100d52626db32e2b1a3c6115235ee817ff97178:src/master_sa.erl

	Prefs = lists:sort(fun(A,B)-> A>B end,
		if
			LogLen =< 2 -> get_pref2();
			LogLen =< 4 -> get_pref4();
			LogLen =< 6 -> get_pref6();
			true -> throw(too_large_sequence)
		end),

	Sink = spawn(?MODULE,merge_sa,[self(),Prefs]),
	%lists:foreach(fun(_) -> spawn(worker,sa,[self(),Sink,File]) end, lists:seq(1,Worker_nbr)),
	send_prefs(Prefs,File,Sink,Worker_nbr),
	receive SA -> [Size|SA] end.




get_pref2() -> [ [B1,B2] || B1 <- "ACGT", B2 <- "$ACGT" ].
get_pref4() -> [ [B1,B2,B3,B4] || B1<-"ACGT",B2<-"ACGT",B3<-"ACGT",B4 <-"$ACGT" ].
get_pref6() -> [ [B1,B2,B3,B4,B5,B6] || B1<-"ACGT",B2<-"ACGT",B3<-"ACGT",B4 <-"ACGT",B5<-"ACGT",B6<-"$ACGT" ].



send_prefs([Prefix|Prefs],File,Sink,Worker_nbr) -> 
	receive 
		{Worker_pid,ready} -> Worker_pid ! Prefix;
		{init, Pid} -> Pid ! {File,Sink}
	end,
	io:format(" FAN: prefix ~p sent~n",[Prefix]),
	send_prefs(Prefs,File,Sink,Worker_nbr);
send_prefs([],_,_,0) -> 
	io:format("All prefixes distributed~n");
send_prefs([],File,Sink,Worker_nbr) -> 
	receive {Worker_pid,ready} -> Worker_pid ! stop end,
	send_prefs([],File,Sink,Worker_nbr-1).




merge_sa(Pid,Prefs) -> 
	%io:format(" SINK: started~n"),
	merge_sa(Pid,Prefs,[]).

merge_sa(Pid,[Prefix|Prefs],Acc) ->
	receive
		{Prefix,SA} -> 
			%io:format(" SINK: received ~p~n",[Prefix]),
			merge_sa(Pid,Prefs,SA++Acc)
	end;
merge_sa(Pid,[],Acc) -> Pid ! Acc.





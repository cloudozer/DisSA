% distributed Suffix array construction
%
% 
% Cloudozer(c), 2015
%

-module(master).
-export([
		t/0,
		main/2,
		merge_sa/2
		]).

-define(SORTED_LEN,200000).



t() -> 
	Tests = [
		"21"
		%"GL000207.1",
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
	File = "data/"++Chromo++".ref",
	Worker_nbr = 5,
	main(File,Worker_nbr),
	%{ok,Bin} = file:read_file(File),
	%io:format("Test passed: ~p~n",[DSA1==sa(binary_to_list(Bin))]),
	t(Tests);
t([]) -> ok.


main(File,Worker_nbr) ->
	Size = filelib:file_size(File),
	LogLen = round(math:log(Size / ?SORTED_LEN) / math:log(4)),

	Prefs = lists:sort(fun(A,B)-> A>B end,
		if
			LogLen =< 2 -> get_pref2();
			LogLen =< 4 -> get_pref4();
			LogLen =< 6 -> get_pref6();
			true -> throw(too_large_sequence)
		end),

	Sink = spawn(?MODULE,merge_sa,[self(),Prefs]),
	lists:foreach(fun(_) -> spawn(worker,sa,[self(),Sink,File]) end, lists:seq(1,Worker_nbr)),
	send_prefs(Prefs,Worker_nbr),
	receive SA -> [Size|SA] end.




get_pref2() -> [ [B1,B2] || B1 <- "ACGT", B2 <- "$ACGT" ].
get_pref4() -> [ [B1,B2,B3,B4] || B1<-"ACGT",B2<-"ACGT",B3<-"ACGT",B4 <-"$ACGT" ].
get_pref6() -> [ [B1,B2,B3,B4,B5,B6] || B1<-"ACGT",B2<-"ACGT",B3<-"ACGT",B4 <-"ACGT",B5<-"ACGT",B6<-"$ACGT" ].



send_prefs([Prefix|Prefs],Worker_nbr) -> 
	receive {Worker_pid,ready} -> Worker_pid ! Prefix end,
	io:format(" FAN: prefix ~p sent~n",[Prefix]),
	send_prefs(Prefs,Worker_nbr);
send_prefs([],0) -> 
	io:format("All prefixes distributed~n");
send_prefs([],Worker_nbr) -> 
	receive {Worker_pid,ready} -> Worker_pid ! stop end,
	send_prefs([],Worker_nbr-1).




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



sa(X) -> [ N || {_,N,_} <- lists:sort(get_suffs(X)) ].



get_suffs(X) ->
	X1 = lists:reverse([$$|lists:reverse(X)]),
	get_suffs([],0,X1,$$).

get_suffs(Acc, N, [H|X],P) ->
	get_suffs([{[H|X],N,P}|Acc], N+1, X, H);
get_suffs(Acc,_,[],_) -> 
	%io:format("Suffices:~n~p~n",[Acc]),
	Acc.


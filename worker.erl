% distributed Suffix array construction
%
% 
% Cloudozer(c), 2015
%

-module(worker).
-export([
		sa/3
		]).



sa(Fan,Sink,File) ->
	%% read file and create Bin and sort fun
	{ok,Bin} = file:read_file(File),
	Size = size(Bin),
	Bin_index = fun(N) when N < Size -> binary:at(Bin, N); (_) -> $$ end,
	F = fun(F,X1,X2) -> V1 = Bin_index(X1), V2 = Bin_index(X2),
						case V1 =:= V2 of true -> F(F,X1+1,X2+1); _ -> V1 < V2 end
		end,	
	Compare = fun(Shift,Ls) -> 
				lists:sort(fun({_,_,X1},{_,_,X2}) -> F(F,X1+Shift,X2+Shift) end,Ls) 
	   		  end,
		

	%% request workload until 'stop' msg comes
	Fan ! {self(),ready},
	sa(Fan,Sink,Bin,Compare).

sa(Fan,Sink,Bin,F) ->
	receive
		stop -> io:format("worker terminated normally~n");
		Prefix -> 
			Sink ! { Prefix, F(length(Prefix),scan(Prefix,Bin)) },
			Fan ! {self(),ready},
			sa(Fan,Sink,Bin,F)
	end.


% returns a string of pointers
scan(Prefix,Bin) -> 
	case lists:last(Prefix) of
		$$ -> 
			LastPart = binary:part( Bin,{byte_size(Bin),1-length(Prefix)} ),
			%io:format("LastPart:~p~n",[LastPart]),
			case list_to_binary(lists:droplast(Prefix)) of 
				LastPart -> [size(Bin)+1-length(Prefix)];
				_ -> []
			end;
		_ -> scan(Prefix,Bin,Prefix,$$,0,[])
	end.

scan(_,<<>>,_,_,_,Acc) -> Acc;
scan(Prefix,<<S,Bin/binary>>,[S|Rest],Prev,N,Acc) -> scan(Prefix,Bin,Prefix,S,N+1, case match(Bin,Rest) of
																				true -> [{S,Prev,N}|Acc];
																				_ -> Acc
																			end);
scan(Prefix,<<S,Bin/binary>>,_,_,N,Acc) -> scan(Prefix,Bin,Prefix,S,N+1,Acc).


match(Bin,Str) when size(Bin) < length(Str) -> false; 
match(Bin,Str) -> binary:part(Bin, 0, length(Str)) =:= list_to_binary(Str).


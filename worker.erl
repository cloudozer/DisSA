% distributed Suffix array construction
%
% 
% Cloudozer(c), 2015
%

-module(worker).
-export([
		sa/2
		]).





sa(Pid,File) ->
	%% read file and create Bin and sort fun
	{ok,Bin} = file:read_file(File),
	Size = size(Bin),
	Bin_index = fun(N) when N < Size-1 -> binary:at(Bin, N); (_) -> $$ end,
	F = fun(F,X1,X2) -> Vx = Bin_index(X1), Vy = Bin_index(X2),
						case Vx =:= Vy of true -> F(F,X1+1,X2+1); _ -> Vx < Vy end
		end,	
	Compare = fun(Shift,Ls) -> 
				lists:sort(fun({_,_,X1},{_,_,X2}) -> F(F,X1+Shift,X2+Shift) end,Ls) 
	   		  end,
		

	%% request workload until 'stop' msg comes
	Pid ! {self(),ready},
	receive
		stop -> io:format("worker terminated normally~n");
		Prefix -> Pid ! {Prefix,Compare(length(Prefix),scan(Prefix,Bin))}
	end.


% returns a string of pointers
scan(Prefix,Bin) -> scan(Prefix,Bin,Prefix,0,[]).

scan(Prefix,<<S,Bin/binary>>,[S|Rest],N,Acc) -> scan(Prefix,Bin,Rest,N+1,Acc);
scan(Prefix,Bin,[],N,Acc) -> scan(Prefix,Bin,Rest,N,[N-length(Prefix)|Acc]);

scan(Prefix,<<$$>>,_,N,Acc) -> Acc;
scan(Prefix,<<_,Bin/binary>>,_,N,Acc) -> scan(Prefix,Bin,Prefix,N+1,Acc).






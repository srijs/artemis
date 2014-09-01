-module(artemis_queue).
-behaviour(gen_server).

% internal gen_server callback functions
-export([init/1, handle_call/3, handle_cast/2]).
-export([handle_info/2, terminate/2, code_change/3]).

% simple public interface
-export([start/0, put/1, peek/0]).



start() -> gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

put(Job) -> gen_server:call(?MODULE, {put, Job}).

peek() -> gen_server:call(?MODULE, {peek}).



init(_) ->
  {ok, {dict:new(), gb_trees:empty()}}.

handle_call({put, Job}, _, State) ->
  case insert_job(Job, State) of
    exists   -> {reply, exists,  State};
    NewState -> {reply, success, NewState}
  end;
handle_call({peek}, _, State) ->
  case peek_job(State) of
    empty -> {reply, empty, State};
    Job   -> {reply, Job, State}
  end.

handle_cast(_, State) ->
  {noreply, State}.

handle_info(_, State) ->
  {noreply, State}.

terminate(_, _) ->
  terminate.

code_change(_, State, _) ->
  {ok, State}.

peek_job({_, Tree}) ->
  case gb_trees:is_empty(Tree) of
    true  -> empty;
    false -> {_, [Job|_]} = gb_trees:smallest(Tree), Job
  end.

insert_job(Job = {Id, _, _, _}, {Dict, Tree}) ->
  case dict:is_key(Id, Dict) of
    true  -> exists;
    false -> {insert_job_into_dict(Job, Dict),
              insert_job_into_tree(Job, Tree)}
  end.

insert_job_into_dict(Job = {Id, _, _, _}, Dict) ->
  dict:store(Id, Job, Dict).

insert_job_into_tree(Job = {_, Prio, _ ,_}, Tree) ->
  List = case gb_trees:lookup(Prio, Tree) of
    none  -> [];
    [H|T] -> [H|T]
  end,
  gb_trees:enter(Prio, [Job|List], Tree).

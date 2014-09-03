-module(artemis_queue).
-behaviour(gen_server).

-include("records.hrl").

-record(state, {dict, tree}).

% internal gen_server callback functions
-export([init/1, handle_call/3, handle_cast/2]).
-export([handle_info/2, terminate/2, code_change/3]).

% simple public interface
-export([start/0, put/2, get/2, pop/1, peek/1]).



start() -> gen_server:start_link(artemis_queue, [], []).

put(Pid, Job) -> gen_server:call(Pid, {put, Job}).

get(Pid, Id) -> gen_server:call(Pid, {get, Id}).

pop(Pid) -> gen_server:call(Pid, {pop}).

peek(Pid) -> gen_server:call(Pid, {peek}).



init(_) ->
  {ok, #state{dict=dict:new(), tree=gb_trees:empty()}}.

handle_call({put, Job}, _, State) ->
  case insert_job(Job, State) of
    exists   -> {reply, exists,  State};
    NewState -> {reply, success, NewState}
  end;
handle_call({get, Id}, _, State) ->
  case get_job(Id, State) of
    error -> {reply, error, State};
    Job   -> {reply, Job,   State}
  end;
handle_call({pop}, _, State) ->
  case take_job(State) of
    empty           -> {reply, empty, State};
    {Job, NewState} -> {reply, Job,   NewState}
  end;
handle_call({peek}, _, State) ->
  case peek_job(State) of
    empty -> {reply, empty, State};
    Job   -> {reply, Job,   State}
  end.

handle_cast(_, State) ->
  {noreply, State}.

handle_info(_, State) ->
  {noreply, State}.

terminate(_, _) ->
  terminate.

code_change(_, State, _) ->
  {ok, State}.


% peek at the next job in the queue

peek_job(State = #state{tree=Tree}) ->
  case gb_trees:is_empty(Tree) of
    true  -> empty;
    false -> peek_job_nonempty(State)
  end.

peek_job_nonempty(#state{dict=Dict, tree=Tree}) ->
  {_, Queue} = gb_trees:smallest(Tree),
  Id = queue:get(Queue),
  dict:fetch(Id, Dict).


% pop the next job from the queue

take_job(State = #state{tree=Tree}) ->
  case gb_trees:is_empty(Tree) of
    true  -> empty;
    false -> take_job_nonempty(State)
  end.

take_job_nonempty(#state{dict=Dict, tree=Tree}) ->
  {_, Queue} = gb_trees:smallest(Tree),
  {{value, Id}, NewQueue} = queue:out(Queue),
  Job = dict:fetch(Id, Dict),
  {Job, #state{dict=delete_job_from_dict(Job, Dict),
               tree=delete_job_from_tree(Job, NewQueue, Tree)}}.

delete_job_from_tree(#job{prio=Prio}, Queue, Tree) ->
  case queue:is_empty(Queue) of
    true  -> gb_trees:delete(Prio, Tree);
    false -> gb_trees:update(Prio, Queue, Tree)
  end.

delete_job_from_dict(#job{id=Id}, Dict) ->
  dict:erase(Id, Dict).


% retrieve a job by id

get_job(Id, #state{dict=Dict}) ->
  case dict:find(Id, Dict) of
    {ok, Job} -> Job;
    error     -> error
  end.


% insert a job into the queue

insert_job(Job = #job{id=Id}, #state{dict=Dict, tree=Tree}) ->
  case dict:is_key(Id, Dict) of
    true  -> exists;
    false -> #state{dict=insert_job_into_dict(Job, Dict),
                    tree=insert_job_into_tree(Job, Tree)}
  end.

insert_job_into_dict(Job = #job{id=Id}, Dict) ->
  dict:store(Id, Job, Dict).

insert_job_into_tree(#job{id=Id, prio=Prio}, Tree) ->
  Jobs = case gb_trees:lookup(Prio, Tree) of
    none           -> queue:new();
    {value, Queue} -> Queue
  end,
  gb_trees:enter(Prio, queue:in(Id, Jobs), Tree).

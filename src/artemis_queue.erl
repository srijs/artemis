-module(artemis_queue).
-behaviour(gen_server).

% internal gen_server callback functions
-export([init/1, handle_call/3, handle_cast/2]).
-export([handle_info/2, terminate/2, code_change/3]).

% simple public interface
-export([start/0, put/1, get/1, pop/0, peek/0]).



start() -> gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

put(Job) -> gen_server:call(?MODULE, {put, Job}).

get(Id) -> gen_server:call(?MODULE, {get, Id}).

pop() -> gen_server:call(?MODULE, {pop}).

peek() -> gen_server:call(?MODULE, {peek}).



init(_) ->
  {ok, {dict:new(), gb_trees:empty()}}.

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

peek_job(State = {_, Tree}) ->
  case gb_trees:is_empty(Tree) of
    true  -> empty;
    false -> peek_job_nonempty(State)
  end.

peek_job_nonempty({Dict, Tree}) ->
  {_, Queue} = gb_trees:smallest(Tree),
  Id = queue:get(Queue),
  dict:fetch(Id, Dict).


% pop the next job from the queue

take_job(State = {_, Tree}) ->
  case gb_trees:is_empty(Tree) of
    true  -> empty;
    false -> take_job_nonempty(State)
  end.

take_job_nonempty({Dict, Tree}) ->
  {_, Queue} = gb_trees:smallest(Tree),
  {{value, Id}, NewQueue} = queue:out(Queue),
  Job = dict:fetch(Id, Dict),
  {Job, {delete_job_from_dict(Job, Dict),
         delete_job_from_tree(Job, NewQueue, Tree)}}.

delete_job_from_tree({_, Prio, _, _}, Queue, Tree) ->
  case queue:is_empty(Queue) of
    true  -> gb_trees:delete(Prio, Tree);
    false -> gb_trees:update(Prio, Queue, Tree)
  end.

delete_job_from_dict({Id, _, _, _}, Dict) ->
  dict:erase(Id, Dict).


% retrieve a job by id

get_job(Id, {Dict, _}) ->
  case dict:find(Id, Dict) of
    {ok, Job} -> Job;
    error     -> error
  end.


% insert a job into the queue

insert_job(Job = {Id, _, _, _}, {Dict, Tree}) ->
  case dict:is_key(Id, Dict) of
    true  -> exists;
    false -> {insert_job_into_dict(Job, Dict),
              insert_job_into_tree(Job, Tree)}
  end.

insert_job_into_dict(Job = {Id, _, _, _}, Dict) ->
  dict:store(Id, Job, Dict).

insert_job_into_tree({Id, Prio, _ ,_}, Tree) ->
  Jobs = case gb_trees:lookup(Prio, Tree) of
    none           -> queue:new();
    {value, Queue} -> Queue
  end,
  gb_trees:enter(Prio, queue:in(Id, Jobs), Tree).

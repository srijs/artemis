-module(artemis_tube).
-behavior(gen_server).

% internal gen_server callback functions
-export([init/1, handle_call/3, handle_cast/2]).
-export([handle_info/2, terminate/2, code_change/3]).



init(_) ->
  process_flag(trap_exit, true),
  Watchers = queue:new(),
  DeadWatchers = sets:new(),
  {ok, ReadyQueue} = artemis_queue:start(),
  {ok, {ReadyQueue, Watchers, DeadWatchers}}.

handle_call({put, Job}, _, {ReadyQueue, Watchers, DeadWatchers}) ->
  Result = artemis_queue:put(ReadyQueue, Job),
  {NewWatchers, NewDeadWatchers} = case Result of
    success -> notify(ReadyQueue, Watchers, DeadWatchers);
    _ -> {Watchers, DeadWatchers}
  end,
  {reply, Result, {ReadyQueue, NewWatchers, NewDeadWatchers}};

handle_call({watch}, {Pid, _}, State = {ReadyQueue, Watchers, DeadWatchers}) ->
  case watch_and_pop(Pid, Watchers, ReadyQueue) of
    exists -> {reply, exists, State};
    NewWatchers -> {reply, ok, {ReadyQueue, NewWatchers, DeadWatchers}}
  end.

handle_cast(_, State) ->
  {noreply, State}.

handle_info({'EXIT', Pid, _}, State = {ReadyQueue, Watchers, DeadWatchers}) ->
  case queue:member(Pid, Watchers) of
    true -> {noreply, {ReadyQueue, Watchers, sets:add_element(Pid, DeadWatchers)}};
    false -> {noreply, State}
  end.

terminate(_, _) ->
  terminate.

code_change(_, State, _) ->
  {ok, State}.



watch_and_pop(Pid, Watchers, ReadyQueue) ->
  case artemis_queue:pop(ReadyQueue) of
    empty -> watch_and_pop_nonimmediate(Pid, Watchers);
    Job -> Pid ! Job, Watchers
  end.

watch_and_pop_nonimmediate(Pid, Watchers) ->
  link(Pid),
  case queue:member(Pid, Watchers) of
    true -> exists;
    false -> queue:in(Pid, Watchers)
  end.


notify(ReadyQueue, Watchers, DeadWatchers) ->
  case queue:peek(Watchers) of
    empty -> {Watchers, DeadWatchers};
    {value, Pid} -> notify_nonempty(ReadyQueue, Pid, Watchers, DeadWatchers)
  end.

notify_nonempty(ReadyQueue, Pid, Watchers, DeadWatchers) ->
  case sets:is_element(Pid, DeadWatchers) of
    true -> notify_skipdead(ReadyQueue, Pid, queue:drop(Watchers), DeadWatchers);
    false -> {notify_nondead(ReadyQueue, Pid, Watchers), DeadWatchers}
  end.

notify_skipdead(ReadyQueue, Pid, Watchers, DeadWatchers) ->
  NewDeadWatchers = case queue:member(Pid, Watchers) of
    true -> DeadWatchers;
    false -> sets:del_element(Pid, DeadWatchers)
  end,
  notify(ReadyQueue, Watchers, NewDeadWatchers).

notify_nondead(ReadyQueue, Pid, Watchers) ->
  case artemis_queue:pop(ReadyQueue) of
    empty -> Watchers;
    Job -> Pid ! Job, queue:drop(Watchers)
  end.

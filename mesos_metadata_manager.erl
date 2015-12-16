-module(mesos_metadata_manager).

-behavior(gen_server).

-export([
         start_link/2,
         stop/0,
         get_root_node/0,
         get_node/1,
         make_empty_child/2,
         delete_children/1
        ]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-export([test/0]).

-record(state, {
          conn,
          namespace
         }).

-define(BASE_NS, "/riak/frameworks").

%% public API

-spec start_link([{string(), integer()}], string()) -> {ok, pid()}.
start_link(ZooKeeperServers, FrameworkID) ->
    gen_server:start_link({local,?MODULE}, ?MODULE, [ZooKeeperServers, FrameworkID], []).

stop() ->
    gen_server:call(?MODULE, stop).

get_root_node() ->
    gen_server:call(?MODULE, get_root_node).

get_node(Node) ->
    gen_server:call(?MODULE, {get_node, Node}).

make_empty_child(Parent, Child) ->
    gen_server:call(?MODULE, {make_empty_child, Parent, Child}).

delete_children(Parent) ->
    gen_server:call(?MODULE, {delete_children, Parent}).

%% gen_server implementation

init([ZooKeeperServers, FrameworkID]) ->
    {ok, ConnPid} = erlzk:connect(ZooKeeperServers, 30000),
    Namespace = string:join([?BASE_NS, FrameworkID], "/"),
    {ok, #state{
            conn = ConnPid,
            namespace = Namespace
           }}.

handle_call(get_root_node, _From, State) ->
    #state{conn = Conn, namespace = Root} = State,
    {reply, get_node(Conn, Root), State};
handle_call({get_node, Node}, _From, State) ->
    Conn = State#state.conn,
    {reply, get_node(Conn, Node), State};
handle_call({make_empty_child, _Parent, _Child}, _From, State) ->
    {reply, unimplemented, State};
handle_call({delete_children, _Parent}, _From, State) ->
    {reply, unimplemented, State};
handle_call(stop, _From, State) ->
    {stop, normal, ok, State};
handle_call(_Msg, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVersion, State, _Extra) ->
    {ok, State}.

%% Private implementation functions:

get_node(_Conn, _Root) ->
    {error, unimplemented}.

%% Wrapper for running the eunit tests which are contained in a separate module:

-define(TEST_ZK_SERVER, [{"localhost", 2181}]).

test() ->
    application:ensure_all_started(erlzk),
    io:format("Checking that ZooKeeper is running at ~p~n", [?TEST_ZK_SERVER]),
    case erlzk:connect(?TEST_ZK_SERVER, 1000) of
        {ok, Pid} ->
            io:format("Found ZooKeeper, closing connection and beginning tests~n", []),
            erlzk:close(Pid),
            mesos_metadata_manager_tests:run(?TEST_ZK_SERVER);
        {error, Error} ->
            io:format("Failed to connect to ZooKeeper! Error ~p~n", [Error])
    end.


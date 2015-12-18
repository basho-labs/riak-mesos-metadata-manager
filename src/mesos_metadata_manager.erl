-module(mesos_metadata_manager).

-behavior(gen_server).

-compile(export_all).

-export([
         start_link/2,
         stop/0,
         get_root_node/0,
         get_node/1,
         make_child/2,
         make_child/3,
         delete_children/1,
         recursive_delete/1
        ]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

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

make_child(Parent, Child) ->
    make_child(Parent, Child, true).

make_child(Parent, Child, Ephemeral) ->
    gen_server:call(?MODULE, {make_child, Parent, Child, Ephemeral}).

delete_children(Parent) ->
    gen_server:call(?MODULE, {delete_children, Parent}).

recursive_delete(Node) ->
    gen_server:call(?MODULE, {recursive_delete, Node}).

%% gen_server implementation

init([ZooKeeperServers, FrameworkID]) ->
    Namespace = string:join([?BASE_NS, FrameworkID], "/"),

    {ok, Conn} = erlzk:connect(ZooKeeperServers, 30000),
    erlzk:create(Conn, "/"), %% Just in case it doesn't already exist
    guarantee_created(Conn, Namespace),

    {ok, #state{
            conn = Conn,
            namespace = Namespace
           }}.

handle_call(get_root_node, _From, State) ->
    #state{conn = Conn, namespace = Root} = State,
    {reply, get_node(Conn, Root), State};
handle_call({get_node, Node}, _From, State) ->
    Conn = State#state.conn,
    {reply, get_node(Conn, Node), State};
handle_call({make_child, Parent, Child, Ephemeral}, _From, State) ->
    Conn = State#state.conn,
    Node = string:join([Parent, Child], "/"),
    {reply, create(Conn, Node, Ephemeral), State};
handle_call({delete_children, Parent}, _From, State) ->
    Conn = State#state.conn,
    {reply, delete_children(Conn, Parent), State};
handle_call({recursive_delete, Node}, _From, State) ->
    Conn = State#state.conn,
    {reply, recursive_delete(Conn, Node), State};
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

create(Conn, Node, Ephemeral) ->
    CreateMode = case Ephemeral of
                     true -> ephemeral;
                     false -> persistent
                 end,
    case erlzk:create(Conn, Node) of
        {ok, _} ->
            {ok, Node, <<>>};
        Error ->
            Error
    end.

guarantee_created(Conn, Node) ->
    Components = string:tokens(Node, "/"),
    lists:foldl(fun(C, Acc) ->
                        NewNode = Acc ++ C,
                        erlzk:create(Conn, NewNode),
                        NewNode ++ "/"
                end, "/", Components).

get_node(Conn, Node) ->
    case erlzk:get_data(Conn, Node) of
        {ok, {Data, _Stat}} ->
            {ok, Node, Data};
        Error ->
            Error
    end.

delete_children(Conn, Parent) ->
    case erlzk:get_children(Conn, Parent) of
        {ok, Children} ->
            lists:foreach(fun(C) -> erlzk:delete(Conn, [Parent, "/", C]) end, Children),
            ok;
        Error ->
            Error
    end.

recursive_delete(Conn, Node) ->
    case erlzk:get_children(Conn, Node) of
        {ok, Children} ->
            [recursive_delete(Conn, [Node, "/", C]) || C <- Children],
            erlzk:delete(Conn, Node);
        {error, no_node} ->
            ok
    end.

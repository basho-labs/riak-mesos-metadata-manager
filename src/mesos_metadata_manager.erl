-module(mesos_metadata_manager).

-behavior(gen_server).

-compile(export_all).

-export([
         start_link/2,
         stop/0,
         get_root_node/0,
         get_node/1,
         get_children/1,
         make_child/2,
         make_child/3,
         make_child_with_data/3,
         make_child_with_data/4,
         set_data/2,
         create_or_set/3,
         delete_node/1,
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

-spec stop() -> ok.
stop() ->
    gen_server:call(?MODULE, stop).

-spec get_root_node() -> {ok, string(), binary()}.
get_root_node() ->
    gen_server:call(?MODULE, get_root_node).

-spec get_node(iodata()) -> {ok, string(), binary()} | {error, atom()}.
get_node(Node) ->
    gen_server:call(?MODULE, {get_node, Node}).

-spec get_children(iodata()) -> {ok, [string()]} | {error, atom()}.
get_children(Node) ->
    gen_server:call(?MODULE, {get_children, Node}).

-spec make_child(iodata(), iodata()) -> {ok, string(), binary()} | {error, atom()}.
make_child(Parent, Child) ->
    make_child_with_data(Parent, Child, <<>>).

-spec make_child(iodata(), iodata(), boolean()) -> {ok, string(), binary()} | {error, atom()}.
make_child(Parent, Child, Ephemeral) when is_boolean(Ephemeral) ->
    make_child_with_data(Parent, Child, <<>>, Ephemeral).

-spec make_child_with_data(string(), string(), binary()) ->
    {ok, string(), binary()} | {error, atom()}.
make_child_with_data(Parent, Child, Data) when is_binary(Data) ->
    make_child_with_data(Parent, Child, Data, false).

-spec make_child_with_data(string(), string(), binary(), boolean()) ->
    {ok, string(), binary()} | {error, atom()}.
make_child_with_data(Parent, Child, Data, Ephemeral) ->
    gen_server:call(?MODULE, {make_child, Parent, Child, Data, Ephemeral}).

-spec set_data(iodata(), binary()) -> ok | {error, atom()}.
set_data(Node, Data) ->
    gen_server:call(?MODULE, {set_data, Node, Data}).

-spec create_or_set(iodata(), iodata(), binary()) -> {ok, iodata(), binary()} | {error, atom()}.
create_or_set(Parent, Child, Data) ->
    gen_server:call(?MODULE, {create_or_set, Parent, Child, Data}).

-spec delete_node(iodata()) -> ok | {error, atom()}.
delete_node(Node) ->
    gen_server:call(?MODULE, {delete_node, Node}).

-spec delete_children(iodata()) -> ok | {error, atom()}.
delete_children(Parent) ->
    gen_server:call(?MODULE, {delete_children, Parent}).

-spec recursive_delete(iodata()) -> ok | {error, atom()}.
recursive_delete(Node) ->
    gen_server:call(?MODULE, {recursive_delete, Node}).

%% gen_server implementation

init([ZooKeeperServers, FrameworkID]) ->
    Namespace = string:join([?BASE_NS, FrameworkID], "/"),

    {ok, Conn} = erlzk:connect(ZooKeeperServers, 30000),
    _ = erlzk:create(Conn, "/"), %% Just in case it doesn't already exist
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
handle_call({get_children, Node}, _From, State) ->
    Conn = State#state.conn,
    {reply, get_children(Conn, Node), State};
handle_call({make_child, Parent, Child, Data, Ephemeral}, _From, State) ->
    Conn = State#state.conn,
    Node = string:join([Parent, Child], "/"),
    {reply, create(Conn, Node, Data, Ephemeral), State};
handle_call({set_data, Node, Data}, _From, State) ->
    Conn = State#state.conn,
    {reply, set_data(Conn, Node, Data), State};
handle_call({create_or_set, Parent, Child, Data}, _From, State) ->
    Conn = State#state.conn,
    {reply, create_or_set(Conn, Parent, Child, Data), State};
handle_call({delete_node, Node}, _From, State) ->
    Conn = State#state.conn,
    {reply, erlzk:delete(Conn, Node), State};
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

create(Conn, Node, Data, Ephemeral) ->
    CreateMode = case Ephemeral of
                     true -> ephemeral;
                     false -> persistent
                 end,
    case erlzk:create(Conn, Node, Data, CreateMode) of
        {ok, Path} ->
            {ok, Path, Data};
        Error ->
            Error
    end.

guarantee_created(Conn, Node) ->
    Components = string:tokens(Node, "/"),
    _ = lists:foldl(fun(C, Acc) ->
                            NewNode = Acc ++ C,
                            _ = erlzk:create(Conn, NewNode),
                            NewNode ++ "/"
                    end, "/", Components),
    ok.

get_node(Conn, Node) ->
    case erlzk:get_data(Conn, Node) of
        {ok, {Data, _Stat}} ->
            {ok, Node, Data};
        Error ->
            Error
    end.

get_children(Conn, Node) ->
    erlzk:get_children(Conn, Node).

set_data(Conn, Node, Data) ->
    case erlzk:set_data(Conn, Node, Data) of
        {ok, _Stat} ->
            ok;
        Error ->
            Error
    end.

create_or_set(Conn, Parent, Child, Data) ->
    Node = [Parent, "/", Child],
    case create(Conn, Node, Data, false) of
        {error, node_exists} ->
            case set_data(Conn, Node, Data) of
                ok ->
                    {ok, Node, Data};
                SetResult ->
                    SetResult
            end;
        CreateResult ->
            CreateResult
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
            _ = [recursive_delete(Conn, [Node, "/", C]) || C <- Children],
            ok = erlzk:delete(Conn, Node);
        {error, no_node} ->
            ok
    end.

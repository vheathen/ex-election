defmodule ExElection.Member do
  @moduledoc """

  Initial task description:

  #################################

  Simple group leader election

  - Processes can communicate with themselves only (e.g no ETS)
  - Each process is identified by id = integer()
  - The leader is the process with the greatest id

  - When process joins the group, it can reach any process. It should reply with:
  - pids of processes in the group
  - {pid, id} of the leader

  New process should verify if it should become leader (highest id). If it is
  a leader, it should let know other nodes that there is a new leader.

  - The leader has responsibility to ackowledge all processes in the group.
  - If ping from leader is missing, the node with the highest id should become a leader.

  #################################

  Finaly it should auto-balance

  Try to use (M is already a correct alias):

  {:ok, p12} = M.start_link id: 12
  {:ok, p1} = M.start_link id: 1
  {:ok, p15} = M.start_link id: 15
  {:ok, p3} = M.start_link id: 3
  {:ok, p25} = M.start_link id: 25
  {:ok, p7} = M.start_link id: 7

  # wait till election is done, console should get 5 debug log messages from node 25 every 5 seconds
  # you can check process states with:

  M.show_state p12
  M.show_state p1

  M.show_state p25

  # it is possible to clear a certain process members list to check if its come back from the leader:

  M.clear_members p1
  M.show_state p1

  # Also we can kill any member:

  M.show_state p25
  GenServer.stop p1
  M.show_state p25

  # We can stop the leader and see what's happened:
  GenServer.stop p25

  # wait till reelection, console should get debug log messages from node 15

  M.show_state p12
  M.show_state p15

  # also we can add new members any time:

  {:ok, p29} = M.start_link id: 29
  {:ok, p27} = M.start_link id: 27
  {:ok, p28} = M.start_link id: 28
  {:ok, p26} = M.start_link id: 26

  M.show_state p29

  # Process registry:

  Registry.lookup(Registry.Members, :members)


  """

  use GenServer

  require Logger

  @update_interval 5_000
  @watchdog_timeout @update_interval + 1_000
  @connection_timeout 5_000

  defmodule State do
    @type t() ::
            %{
              self: member(),
              members: [member()],
              leader: member(),
              election_watchdog: reference(),
              update_timer: reference(),
              member_refs: %{reference() => member()}
            }

    @type member :: %{pid: pid(), id: non_neg_integer()} | nil

    defstruct self: nil,
              members: [],
              leader: nil,
              election_watchdog: nil,
              update_timer: nil,
              member_refs: %{}
  end

  def start_link(opts) do
    id = Keyword.get(opts, :id)
    GenServer.start_link(__MODULE__, id, name: name(id))
  end

  #####
  #
  # Functions for debug proposes
  #
  #####
  def show_state(pid), do: GenServer.call(pid, :show_state)
  def clear_members(pid), do: GenServer.call(pid, :clear_members)

  #
  # Private API
  #

  @impl true
  def init(id) do
    {:ok, _} = Registry.register(Registry.Members, :members, id)

    Logger.debug("Init")

    {:ok, %{%State{} | self: %{pid: self(), id: id}}, {:continue, :get_members}}
  end

  @impl true
  def handle_continue(:get_members, state) do
    get_members(state)
  end

  def handle_continue(:find_leader, state) do
    find_leader(state)
  end

  def handle_continue(:maybe_start_elections, state) do
    maybe_start_elections(state)
  end

  def handle_continue(:do_elections, state) do
    do_elections(state)
  end

  @impl true
  def handle_call(:show_state, _from, state) do
    {:reply, state, state}
  end

  def handle_call(:clear_members, _from, state) do
    {:reply, :ok, %{state | members: []}}
  end

  def handle_call({:join_group, member}, _from, state) do
    members =
      if member in state.members,
        do: state.members,
        else: [member | state.members]

    state =
      state
      |> demonitor_members()
      |> Map.put(:members, members)
      |> monitor_members()

    {:reply, {:ok, state}, state}
  end

  def handle_call(:get_state, _from, state) do
    {:reply, state, state}
  end

  def handle_call({:take_over, member}, _from, state) do
    maybe_take_over(state, member)
  end

  @impl true
  def handle_cast({:update_member_state, new_state}, state),
    do: {:noreply, update_member_state(state, new_state)}

  @impl true
  # catch-all for timeouts
  def handle_info({ref, _}, state) when is_reference(ref), do: {:noreply, state}

  # monitoring members
  def handle_info({:DOWN, ref, :process, _pid, _reason}, %State{} = state) do
    {:noreply, handle_member_down(state, ref)}
  end

  def handle_info(:watchdog, state) do
    react_on_watchdog(state)
  end

  def handle_info(:get_members, state) do
    get_members(state)
  end

  def handle_info(:push_state_to_members, state) do
    push_state_to_members(state)
  end

  #
  # Common part
  #
  defp find_leader(%State{self: self, members: [_]} = state) do
    Logger.debug("#{inspect(state.self.id)}: find_leader: [_]: #{inspect(state)}")

    Process.send_after(self(), :get_members, @update_interval)

    {:noreply, %{state | leader: self}, {:continue, :maybe_start_elections}}
  end

  defp find_leader(%State{self: self, members: members} = state) do
    Logger.debug("#{inspect(state.self.id)}: find_leader: #{inspect(state)}")

    state =
      members
      |> Enum.reduce_while(
        # if no members answer then current member must become a leader
        %{state | leader: self},

        #
        fn
          ^self, state ->
            {:cont, state}

          member, state ->
            try do
              {:halt, update_member_state(state, get_state(member))}
            catch
              :exit, _ ->
                {:cont, state}
            end
        end
      )

    {:noreply, state, {:continue, :maybe_start_elections}}
  end

  defp get_members(%{members: [_, _ | _]} = state) do
    Logger.debug("#{inspect(state.self.id)}: get_members [_, _ | _]:")

    {:noreply, state, {:continue, :find_leader}}
  end

  defp get_members(%{} = state) do
    Logger.debug("#{inspect(state.self.id)}: get_members small")

    members =
      Registry.Members
      |> Registry.lookup(:members)
      |> Enum.map(fn {pid, id} -> %{pid: pid, id: id} end)

    {:noreply, %{state | members: members}, {:continue, :find_leader}}
  end

  defp maybe_start_elections(%State{self: self, leader: self} = state) do
    Logger.debug(
      "#{inspect(state.self.id)}: maybe_start_elections(%State{self: self, leader: self})"
    )

    {:noreply, take_leadership(state)}
  end

  defp maybe_start_elections(%State{} = state) do
    Logger.debug("#{inspect(state.self.id)}: maybe_start_elections(%State{})")

    {:noreply, state, {:continue, :do_elections}}
  end

  defp do_elections(%State{self: self, leader: self} = state) do
    Logger.debug(
      "#{inspect(state.self.id)}: do_elections(%State{self: self, leader: self} = state)"
    )

    {:noreply, take_leadership(state)}
  end

  defp do_elections(%State{leader: nil} = state) do
    Logger.debug("#{inspect(state.self.id)}: do_elections(%State{leader: nil} = state)")

    {:noreply, state, {:continue, :find_leader}}
  end

  defp do_elections(%State{self: %{id: id} = self, leader: %{id: leader_id} = leader} = state)
       when id > leader_id do
    Logger.debug("#{inspect(state.self.id)}: do_elections when bigger")

    try do
      case take_over(leader, self) do
        {:ok, :you_won, members} ->
          %{state | leader: self, members: members}

        {:error, :disagree, new_state} ->
          state
          |> update_member_state(new_state)
          |> notify_leader()
      end
    catch
      :exit, _ ->
        %{state | leader: nil}
    end
    |> do_elections()
  end

  defp do_elections(state) do
    Logger.debug("#{inspect(state.self.id)}: do_elections()")

    {:noreply, notify_leader(state)}
  end

  #
  # group member part
  #
  defp update_member_state(
         %State{self: %{id: self_id} = self} = state,
         %State{self: source, leader: %{id: leader_id}} = leader_state
       )
       when self_id < leader_id do
    Logger.debug(
      "#{inspect(state.self.id)}: update_member_state normal from #{inspect(source.id)}"
    )

    cancel_update_timer(state)

    if state.election_watchdog, do: Process.cancel_timer(state.election_watchdog)
    timer = Process.send_after(self(), :watchdog, @watchdog_timeout)

    %State{
      leader_state
      | self: self,
        member_refs: %{},
        update_timer: nil,
        election_watchdog: timer
    }
  end

  defp update_member_state(state, not_a_leader_state) do
    Logger.debug("#{inspect(state.self.id)}: update_member_state(state, _leader_state)}")

    push_state_to_member(not_a_leader_state.self, state)

    state
  end

  defp notify_leader(%State{leader: leader, self: self} = state) do
    {:ok, new_state} = join_group(leader, self)

    update_member_state(state, new_state)
  end

  defp react_on_watchdog(%State{self: %{id: id}, leader: %{id: id}} = state) do
    {:noreply, state}
  end

  defp react_on_watchdog(%State{} = state) do
    {:noreply, %{state | leader: nil}, {:continue, :maybe_start_elections}}
  end

  #
  # Group leader part
  #
  defp handle_member_down(%State{} = state, ref) do
    member = Map.get(state.member_refs, ref)

    Logger.debug(
      "#{inspect(state.self.id)}: handle_member_down: node #{member && member.id} down"
    )

    is_ref = fn
      {^ref, _} -> true
      _ -> false
    end

    is_member = fn
      ^member -> true
      _ -> false
    end

    %{
      state
      | member_refs: state.member_refs |> Enum.reject(&is_ref.(&1)) |> Map.new(),
        members: Enum.reject(state.members, &is_member.(&1))
    }
  end

  defp take_leadership(%State{self: self, members: members} = state) do
    Logger.debug("#{inspect(state.self.id)}: take_leadership: #{inspect(state)}")

    if self in members do
      state
    else
      %{state | members: [self | members]}
    end
    |> monitor_members()
    |> reschedule_members_update()
  end

  defp monitor_members(state) do
    %{state | member_refs: state.members |> Enum.map(&{Process.monitor(&1.pid), &1}) |> Map.new()}
  end

  defp demonitor_members(state) do
    Enum.each(state.member_refs, fn {ref, _} -> Process.demonitor(ref) end)

    %{state | member_refs: %{}}
  end

  defp push_state_to_members(%State{leader: leader, members: members} = state) do
    members
    |> Enum.reject(&(&1 == leader))
    |> Enum.each(&push_state_to_member(&1, state))

    state = reschedule_members_update(state)

    {:noreply, state}
  end

  defp push_state_to_member(member, state),
    do: GenServer.cast(member.pid, {:update_member_state, state})

  defp reschedule_members_update(%State{} = state) do
    if state.update_timer, do: Process.cancel_timer(state.update_timer)

    %{state | update_timer: Process.send_after(self(), :push_state_to_members, @update_interval)}
  end

  defp maybe_take_over(%State{leader: %{id: leader_id}} = state, %{id: member_id} = member)
       when member_id > leader_id do
    cancel_update_timer(state)

    {:reply, {:ok, :you_won, state.members}, demonitor_members(%{state | leader: member})}
  end

  defp maybe_take_over(state, _member) do
    {:reply, {:error, :disagree, state}, state}
  end

  defp cancel_update_timer(state) do
    if state.update_timer, do: Process.cancel_timer(state.update_timer)
  end

  #
  #
  #

  @spec get_state(State.member()) :: %State{}
  defp get_state(member), do: GenServer.call(member.pid, :get_state, @connection_timeout)

  defp take_over(leader, self),
    do: GenServer.call(leader.pid, {:take_over, self}, @connection_timeout)

  defp join_group(leader, member), do: GenServer.call(leader.pid, {:join_group, member})

  defp name(id), do: {:via, Registry, {Registry.Processes, id}}
end

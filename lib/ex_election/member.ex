defmodule ExElection.Member do
  use GenServer

  require Logger

  @group_update_interval 5_000
  @leader_watchdog_timeout @group_update_interval + 1_000
  @connection_timeout 5_000

  alias __MODULE__, as: Member

  @type t ::
          %__MODULE__{
            ref: member_reference(),
            id: non_neg_integer()
          }

  @type member_reference :: pid() | atom() | term()

  @enforce_keys [:ref, :id]

  defstruct ref: nil,
            id: nil

  defmodule Message do
    alias ExElection.Member

    @type t :: %__MODULE__{
            leader: Member.t(),
            members: [Member.member_reference()],
            from: Member.t()
          }

    @enforce_keys [:leader, :members, :from]

    defstruct members: [],
              leader: nil,
              from: nil
  end

  defmodule State do
    alias ExElection.Member

    @type t ::
            %__MODULE__{
              self: Member.t(),
              leader: Member.t(),
              previous_leader: Member.t(),
              members: [Member.member_reference()],
              member_refs: %{reference() => Member.member_reference()},
              leader_watchdog: reference(),
              group_update_timer: reference(),

              # only for process state inspection,
              # it updates when a group member receives a message with new group info
              updated_at: DateTime.t()
            }

    @type member :: %{pid: pid(), id: non_neg_integer()} | nil

    defstruct self: nil,
              leader: nil,
              previous_leader: nil,
              members: [],
              member_refs: %{},
              leader_watchdog: nil,
              group_update_timer: nil,
              updated_at: nil
  end

  @spec start_link(id :: non_neg_integer()) :: GenServer.on_start()
  def start_link(id) do
    GenServer.start_link(__MODULE__, id, name: name(id))
  end

  @spec name(id :: non_neg_integer()) :: member_reference()
  def name(id), do: {:via, Registry, {GroupMembers, id}}

  @spec inspect_state(member :: member_reference()) :: State.t()
  def inspect_state(member), do: GenServer.call(member, :return_state)

  @spec clear_members(member :: member_reference()) :: :ok
  def clear_members(member), do: GenServer.call(member, :clear_members)

  @spec inspect_registry :: list({non_neg_integer(), pid()})
  def inspect_registry,
    do: Registry.select(GroupMembers, [{{:"$1", :"$2", :"$3"}, [], [{{:"$1", :"$2"}}]}])

  #
  # Internal implementation
  #

  @impl true
  def init(id),
    do: {:ok, %State{self: %Member{ref: self(), id: id}}, {:continue, :find_group_state}}

  @impl true
  def handle_continue(:find_group_state, %State{} = state) do
    {
      :noreply,
      state
      |> fetch_group_members_from_registry()
      |> find_group_state(),
      {:continue, :handle_state_changes}
    }
  end

  def handle_continue(:handle_state_changes, %State{self: self, leader: self} = state),
    do: {:noreply, state, {:continue, :become_the_leader}}

  def handle_continue(
        :handle_state_changes,
        %State{self: %{id: id}, leader: %{id: leader_id}} = state
      )
      when id > leader_id do
    {
      :noreply,
      %{state | leader: state.self, previous_leader: state.leader},
      {:continue, :handle_state_changes}
    }
  end

  def handle_continue(:handle_state_changes, %State{self: self, previous_leader: self} = state),
    do: {:noreply, state, {:continue, :step_down}}

  def handle_continue(
        :handle_state_changes,
        %State{leader: leader, previous_leader: leader} = state
      ),
      do: {:noreply, rearm_leader_watchdog(state)}

  def handle_continue(
        :handle_state_changes,
        %State{leader: _leader, previous_leader: _previous_leader} = state
      ) do
    case join_group_leader(state) do
      {:ok, %Message{} = message} ->
        {:noreply, apply_changes(message, state), {:continue, :handle_state_changes}}

      {:error, _} ->
        {:noreply, %{state | leader: nil}, {:continue, :find_group_state}}
    end
  end

  def handle_continue(:become_the_leader, %State{} = state) do
    {
      :noreply,
      state
      |> monitor_members()
      |> schedule_group_notification()
    }
  end

  def handle_continue(:step_down, %State{} = state) do
    {
      :noreply,
      state
      |> demonitor_members()
      |> stop_group_notification()
      |> Map.put(:previous_leader, nil),
      {:continue, :handle_state_changes}
    }
  end

  @impl true
  def handle_call(:return_state, _from, state), do: {:reply, state, state}
  def handle_call(:clear_members, _from, state), do: {:reply, :ok, %{state | members: []}}

  def handle_call(:get_group_state, _from, %State{} = state),
    do: {:reply, build_message(state), state}

  def handle_call(:new_member_joined, {from_pid, _}, %State{} = state),
    do: {
      :reply,
      build_message(state),
      state
      |> demonitor_members()
      |> maybe_add_to_members(from_pid)
      |> monitor_members()
    }

  @impl true
  def handle_cast(
        {:group_state_updated, %Message{leader: %{id: offered_leader_id}, from: %{ref: from}}},
        %State{leader: %{id: real_leader_id}} = state
      )
      when offered_leader_id < real_leader_id do
    notify_group_member(from, state)

    {:noreply, state, {:continue, :handle_state_changes}}
  end

  def handle_cast({:group_state_updated, %Message{} = message}, %State{} = state),
    do: {:noreply, apply_changes(message, state), {:continue, :handle_state_changes}}

  @impl true
  def handle_info(:leader_watchdog_timeout, %State{self: self, leader: self} = state),
    do: {:noreply, state}

  def handle_info(:leader_watchdog_timeout, %State{} = state),
    do: {
      :noreply,
      %{state | leader: state.self, previous_leader: state.leader},
      {:continue, :handle_state_changes}
    }

  def handle_info(:notify_group_members, %State{leader: %{ref: leader}, members: members} = state) do
    Enum.each(members, fn
      ^leader -> :ok
      member -> notify_group_member(member, state)
    end)

    {:noreply, schedule_group_notification(state)}
  end

  # handle timed out replies
  def handle_info({ref, _}, state) when is_reference(ref), do: {:noreply, state}

  # handle members exits
  def handle_info(
        {:DOWN, ref, :process, _pid, _reason},
        %State{members: members, member_refs: members_refs} = state
      ) do
    member = Map.get(state.member_refs, ref)

    {:noreply,
     %{
       state
       | members: Enum.reject(members, &(&1 == member)),
         member_refs: Map.delete(members_refs, ref)
     }}
  end

  defp fetch_group_members_from_registry(%State{} = state) do
    Map.put(
      state,
      :members,
      Registry.select(GroupMembers, [{{:"$1", :"$2", :"$3"}, [], [:"$2"]}])
    )
  end

  defp find_group_state(%State{self: self, members: [_]} = state),
    do: %{state | leader: self}

  defp find_group_state(%State{self: self, members: members} = state) do
    members
    |> Enum.reduce_while(
      # if no members answer then current member must become a leader
      %{state | leader: self},

      #
      fn
        ^self, state ->
          {:cont, state}

        member_ref, state ->
          case get_group_state(member_ref) do
            {:ok, %Message{} = message} ->
              {:halt, apply_changes(message, state)}

            {:error, _} ->
              {:cont, state}
          end
      end
    )
  end

  @spec get_group_state(member_ref :: member_reference()) ::
          {:ok, Message.t()} | {:error, :timeout}
  defp get_group_state(member_ref) do
    try do
      {:ok, GenServer.call(member_ref, :get_group_state, @connection_timeout)}
    catch
      :exit, _ ->
        {:error, :timeout}
    end
  end

  @spec join_group_leader(state :: State.t()) :: {:ok, Message.t()} | {:error, :timeout}
  defp join_group_leader(%State{leader: %{ref: leader_ref}}) do
    try do
      {:ok, GenServer.call(leader_ref, :new_member_joined, @connection_timeout)}
    catch
      :exit, _ ->
        {:error, :timeout}
    end
  end

  defp apply_changes(
         %Message{members: members, leader: leader, from: from},
         %State{members: members, leader: leader, previous_leader: leader} = state
       ) do
    Logger.debug("#{state.self.id} got update from #{from.id}")

    state |> update_timestamp()
  end

  defp apply_changes(
         %Message{members: members, leader: leader, from: from},
         %State{leader: leader, previous_leader: leader} = state
       ) do
    Logger.debug("#{state.self.id} got update from #{from.id}")

    state
    |> maybe_add_to_members(from.ref, members)
    |> update_timestamp()
  end

  defp apply_changes(
         %Message{members: members, leader: leader, from: from},
         %State{leader: previous_leader} = state
       ) do
    Logger.debug("#{state.self.id} got update from #{from.id}")

    %{
      state
      | leader: leader,
        previous_leader: previous_leader
    }
    |> maybe_add_to_members(from.ref, members)
    |> update_timestamp()
  end

  defp update_timestamp(%State{} = state), do: %{state | updated_at: DateTime.utc_now()}

  defp rearm_leader_watchdog(%State{leader_watchdog: timer_ref} = state) do
    if timer_ref, do: Process.cancel_timer(timer_ref)

    %{
      state
      | leader_watchdog:
          Process.send_after(self(), :leader_watchdog_timeout, @leader_watchdog_timeout)
    }
  end

  defp monitor_members(%State{members: members} = state) do
    %{state | member_refs: Enum.reduce(members, %{}, &Map.put(&2, Process.monitor(&1), &1))}
  end

  defp demonitor_members(%State{member_refs: member_refs} = state) do
    Enum.each(member_refs, fn {ref, _} -> Process.demonitor(ref) end)

    %{state | member_refs: %{}}
  end

  defp schedule_group_notification(%State{group_update_timer: group_update_timer} = state) do
    if group_update_timer, do: Process.cancel_timer(group_update_timer)

    %{
      state
      | group_update_timer:
          Process.send_after(self(), :notify_group_members, @group_update_interval)
    }
  end

  defp stop_group_notification(%State{group_update_timer: group_update_timer} = state) do
    if group_update_timer, do: Process.cancel_timer(group_update_timer)
    state
  end

  defp notify_group_member(member_ref, %State{} = state),
    do: GenServer.cast(member_ref, {:group_state_updated, build_message(state)})

  defp maybe_add_to_members(state, member, members \\ nil)

  defp maybe_add_to_members(%State{members: members} = state, member, nil),
    do: %{state | members: rebuild_members(members, member)}

  defp maybe_add_to_members(%State{} = state, member, members) when is_list(members),
    do: %{state | members: rebuild_members(members, member)}

  defp rebuild_members(members, member), do: (member in members && members) || [member | members]

  defp build_message(%State{self: self, leader: leader, members: members}),
    do: %Message{leader: leader, members: members, from: self}
end

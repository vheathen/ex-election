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

```(elixir)

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

```

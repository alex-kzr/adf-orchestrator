"""Basic structural validation; full production graph semantics are backlog work."""
from collections import deque

NODE_TYPES = {"start", "planner", "agent_task", "condition", "parallel_fork", "parallel_join", "test", "review", "approval", "git_commit", "end"}

def validate_graph(graph: object) -> list[str]:
    if not isinstance(graph, dict) or graph.get("schema_version") != "1":
        return ["Expected a schema_version 1 object"]
    nodes, edges = graph.get("nodes"), graph.get("edges")
    if not isinstance(nodes, list) or not 2 <= len(nodes) <= 100 or not isinstance(edges, list):
        return ["Expected 2..100 nodes and an edges array"]
    if any(not isinstance(n, dict) or not isinstance(n.get("id"), str) or not isinstance(n.get("type"), str) or n.get("type") not in NODE_TYPES for n in nodes):
        return ["Each node needs a string ID and supported type"]
    ids = [n["id"] for n in nodes]
    if len(set(ids)) != len(ids):
        return ["Node IDs must be unique"]
    starts = [n["id"] for n in nodes if n["type"] == "start"]
    ends = [n["id"] for n in nodes if n["type"] == "end"]
    if len(starts) != 1 or len(ends) != 1:
        return ["Exactly one start and end required"]
    outgoing: dict[str, list[str]] = {i: [] for i in ids}
    incoming: dict[str, list[str]] = {i: [] for i in ids}
    for edge in edges:
        if not isinstance(edge, dict) or not isinstance(edge.get("source"), str) or not isinstance(edge.get("target"), str) or edge.get("source") not in outgoing or edge.get("target") not in incoming:
            return ["Edge references unknown node"]
        a, b = edge["source"], edge["target"]
        if b in outgoing[a]:
            return ["Duplicate edge"]
        outgoing[a].append(b)
        incoming[b].append(a)
    degree = {i: len(incoming[i]) for i in ids}
    queue = deque(i for i in ids if degree[i] == 0)
    visited = 0
    while queue:
        item = queue.popleft()
        visited += 1
        for nxt in outgoing[item]:
            degree[nxt] -= 1
            if degree[nxt] == 0:
                queue.append(nxt)
    if visited != len(ids):
        return ["Workflow has a cycle"]
    def reachable(start: str, links: dict[str, list[str]]) -> set[str]:
        seen: set[str] = set()
        todo = [start]
        while todo:
            item = todo.pop()
            if item not in seen:
                seen.add(item)
                todo.extend(links[item])
        return seen
    if reachable(starts[0], outgoing) != set(ids) or reachable(ends[0], incoming) != set(ids):
        return ["All nodes must be reachable from start and able to reach end"]
    return []

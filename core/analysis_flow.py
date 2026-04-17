import math
from collections import defaultdict

from core.analysis_constants import (
    FLOW_PATH_SEPARATOR,
    FLOW_PATTERN_CAP,
    FLOW_LAYOUT_SWEEP_ITERATIONS,
    FLOW_FREQUENCY_ACTIVITY_COLUMN,
    FLOW_FREQUENCY_EVENT_COUNT_COLUMN,
    FLOW_FREQUENCY_CASE_COUNT_COLUMN,
    FLOW_TRANSITION_FROM_COLUMN,
    FLOW_TRANSITION_TO_COLUMN,
    FLOW_TRANSITION_COUNT_COLUMN,
    FLOW_PATTERN_CASE_COUNT_COLUMN,
    FLOW_PATTERN_COLUMN,
    TRANSITION_ANALYSIS_CONFIG,
    FLOW_TRANSITION_AVG_WAIT_COLUMN,
    convert_analysis_result_to_records,
    create_transition_analysis,
)
from core.analysis_core import (
    build_case_pattern_table,
    build_duration_interval_table,
    build_transition_key,
    filter_prepared_df_by_pattern,
    format_duration_text,
)


# -----------------------------------------------------------------------------
# Process map graph construction
# -----------------------------------------------------------------------------

def clamp_flow_percent(percent):
    try:
        numeric_percent = int(percent)
    except (TypeError, ValueError):
        numeric_percent = 0

    return max(0, min(100, numeric_percent))


def _calculate_flow_limit(total_count, percent, minimum=1):
    if total_count <= 0 or percent <= 0:
        return 0

    return min(total_count, max(minimum, math.ceil(total_count * (percent / 100))))


def _parse_pattern_steps(row):
    pattern = str(row.get(FLOW_PATTERN_COLUMN) or "").strip()
    if not pattern:
        return []

    return [
        step.strip()
        for step in pattern.split(FLOW_PATH_SEPARATOR)
        if step.strip()
    ]


def _get_transition_avg_duration_min(row):
    raw_value = (
        row.get(TRANSITION_ANALYSIS_CONFIG["display_columns"].get("avg_duration_min", ""))
        or row.get(FLOW_TRANSITION_AVG_WAIT_COLUMN)
        or row.get("avg_duration_min")
    )
    try:
        return round(float(raw_value or 0.0), 2)
    except (TypeError, ValueError):
        return 0.0


def _build_selected_pattern_prepared_df(prepared_df, selected_pattern_rows):
    if prepared_df is None or prepared_df.empty or not selected_pattern_rows:
        return prepared_df.iloc[0:0].copy() if prepared_df is not None else None

    selected_patterns = {
        str(row.get(FLOW_PATTERN_COLUMN) or "").strip()
        for row in selected_pattern_rows
        if str(row.get(FLOW_PATTERN_COLUMN) or "").strip()
    }
    if not selected_patterns:
        return prepared_df.iloc[0:0].copy()

    case_pattern_df = build_case_pattern_table(prepared_df)
    selected_case_ids = case_pattern_df.loc[
        case_pattern_df["pattern"].isin(selected_patterns),
        "case_id",
    ]
    if selected_case_ids.empty:
        return prepared_df.iloc[0:0].copy()

    return prepared_df[prepared_df["case_id"].isin(selected_case_ids)].copy()


def select_pattern_rows_for_flow(
    pattern_rows,
    pattern_percent=10,
    pattern_count=None,
    pattern_cap=FLOW_PATTERN_CAP,
):
    cap = max(0, int(pattern_cap or 0))
    requested_pattern_percent = clamp_flow_percent(pattern_percent)
    sorted_pattern_rows = sorted(
        pattern_rows,
        key=lambda row: (
            -int(row.get(FLOW_PATTERN_CASE_COUNT_COLUMN) or 0),
            str(row.get(FLOW_PATTERN_COLUMN) or ""),
        ),
    )
    effective_pattern_count = min(len(sorted_pattern_rows), cap)
    requested_pattern_count = None if pattern_count is None else max(0, int(pattern_count or 0))
    if requested_pattern_count is None:
        used_pattern_count = _calculate_flow_limit(
            effective_pattern_count,
            requested_pattern_percent,
        )
    else:
        used_pattern_count = min(effective_pattern_count, requested_pattern_count)

    return {
        "requested_percent": requested_pattern_percent,
        "requested_count": requested_pattern_count,
        "total_pattern_count": len(sorted_pattern_rows),
        "effective_pattern_count": effective_pattern_count,
        "used_pattern_count": used_pattern_count,
        "cap": cap,
        "selected_pattern_rows": sorted_pattern_rows[:used_pattern_count],
    }


def _build_flow_graph(pattern_rows, transition_rows=None, frequency_rows=None):
    transition_rows = transition_rows or []
    frequency_rows = frequency_rows or []
    node_map = {}
    edge_map = {}

    def ensure_node(name):
        node_name = str(name or "").strip()
        if not node_name:
            return None

        if node_name not in node_map:
            node_map[node_name] = {
                "name": node_name,
                "weight": 0,
                "caseWeight": 0,
                "positionTotal": 0,
                "positionWeight": 0,
                "incoming": 0,
                "outgoing": 0,
                "layerScore": 0,
                "layer": 0,
                "orderScore": 0,
            }

        return node_map[node_name]

    for row in frequency_rows:
        activity_name = str(row.get(FLOW_FREQUENCY_ACTIVITY_COLUMN) or "").strip()
        if not activity_name:
            continue

        node = ensure_node(activity_name)
        node["weight"] = max(node["weight"], int(row.get(FLOW_FREQUENCY_EVENT_COUNT_COLUMN) or 0))
        node["caseWeight"] = max(node["caseWeight"], int(row.get(FLOW_FREQUENCY_CASE_COUNT_COLUMN) or 0))

    for row in pattern_rows:
        case_count = int(row.get(FLOW_PATTERN_CASE_COUNT_COLUMN) or 0)
        steps = _parse_pattern_steps(row)

        for step_index, step in enumerate(steps):
            node = ensure_node(step)
            node["positionTotal"] += step_index * case_count
            node["positionWeight"] += case_count

            if node["weight"] == 0:
                node["weight"] = case_count

            if node["caseWeight"] == 0:
                node["caseWeight"] = case_count

            if step_index == len(steps) - 1:
                continue

            next_step = steps[step_index + 1]
            ensure_node(next_step)

            if transition_rows:
                continue

            edge_key = (step, next_step)
            if edge_key not in edge_map:
                edge_map[edge_key] = {
                    "source": step,
                    "target": next_step,
                    "count": 0,
                    "avg_duration_min": 0.0,
                    "avg_duration_sec": 0.0,
                    "avg_duration_text": "",
                }

            edge_map[edge_key]["count"] += case_count

    for row in transition_rows:
        source_name = str(row.get(FLOW_TRANSITION_FROM_COLUMN) or "").strip()
        target_name = str(row.get(FLOW_TRANSITION_TO_COLUMN) or "").strip()
        transition_count = int(row.get(FLOW_TRANSITION_COUNT_COLUMN) or 0)

        if not source_name or not target_name or transition_count <= 0:
            continue

        ensure_node(source_name)
        ensure_node(target_name)

        edge_key = (source_name, target_name)
        if edge_key not in edge_map:
            edge_map[edge_key] = {
                "source": source_name,
                "target": target_name,
                "count": 0,
                "avg_duration_min": 0.0,
                "avg_duration_sec": 0.0,
                "avg_duration_text": "",
            }

        edge_map[edge_key]["count"] = max(edge_map[edge_key]["count"], transition_count)
        avg_duration_min = _get_transition_avg_duration_min(row)
        edge_map[edge_key]["avg_duration_min"] = avg_duration_min
        edge_map[edge_key]["avg_duration_sec"] = round(avg_duration_min * 60, 2)
        edge_map[edge_key]["avg_duration_text"] = format_duration_text(avg_duration_min * 60)

    nodes = list(node_map.values())
    edges = [
        edge
        for edge in edge_map.values()
        if edge["source"] != edge["target"] and edge["count"] > 0
    ]
    node_lookup = {node["name"]: node for node in nodes}

    for edge in edges:
        source_node = node_lookup.get(edge["source"])
        target_node = node_lookup.get(edge["target"])

        if source_node:
            source_node["outgoing"] += edge["count"]

        if target_node:
            target_node["incoming"] += edge["count"]

    for node in nodes:
        if node["positionWeight"] > 0:
            node["layerScore"] = node["positionTotal"] / node["positionWeight"]
        else:
            node["layerScore"] = 0

        node["layer"] = max(0, round(node["layerScore"]))

        if node["weight"] == 0:
            node["weight"] = max(node["incoming"], node["outgoing"], node["caseWeight"], 1)

        if node["caseWeight"] == 0:
            node["caseWeight"] = max(node["incoming"], node["outgoing"], node["weight"], 1)

    return _apply_flow_layout(nodes, edges)


def _filter_flow_graph(nodes, edges, activity_percent=100, connection_percent=100):
    total_node_count = len(nodes)
    total_edge_count = len(edges)

    if not total_node_count:
        return {
            "nodes": [],
            "edges": [],
            "available_activity_count": 0,
            "visible_activity_count": 0,
            "available_connection_count": total_edge_count,
            "visible_connection_count": 0,
        }

    requested_activity_percent = clamp_flow_percent(activity_percent)
    requested_connection_percent = clamp_flow_percent(connection_percent)
    activity_limit = _calculate_flow_limit(
        total_node_count,
        requested_activity_percent,
        minimum=2 if total_node_count > 1 else 1,
    )

    selected_nodes = sorted(
        nodes,
        key=lambda node: (-node["weight"], node["name"]),
    )[:activity_limit]
    selected_node_names = {node["name"] for node in selected_nodes}

    candidate_edges = [
        edge
        for edge in edges
        if edge["source"] in selected_node_names and edge["target"] in selected_node_names
    ]
    connection_limit = _calculate_flow_limit(
        len(candidate_edges),
        requested_connection_percent,
    )
    selected_edges = candidate_edges[:connection_limit]

    # Show all selected nodes regardless of whether they have visible edges.
    # This prevents the flow from going empty when the slider reduces activity_percent
    # to the point where selected nodes have no mutual edges.
    visible_nodes = [
        {
            **node,
        }
        for node in selected_nodes
    ]
    visible_edges = [
        {
            **edge,
        }
        for edge in selected_edges
    ]
    # Re-index orderScore to keep it compact for the subset, but keep layer/weight from parent
    nodes_by_layer = defaultdict(list)
    for n in visible_nodes:
        nodes_by_layer[n["layer"]].append(n)
    for layer in nodes_by_layer:
        nodes_by_layer[layer].sort(key=lambda x: (x.get("orderScore", 0), x["name"]))
        for i, n in enumerate(nodes_by_layer[layer]):
            n["orderScore"] = i

    return {
        "nodes": visible_nodes,
        "edges": visible_edges,
        "available_activity_count": total_node_count,
        "visible_activity_count": len(visible_nodes),
        "available_connection_count": total_edge_count,
        "visible_connection_count": len(visible_edges),
    }


def _reindex_layer_nodes(layer_nodes):
    for index, node in enumerate(layer_nodes):
        node["orderScore"] = index


def _count_edge_crossings(edges, node_lookup):
    crossing_score = 0

    for left_index, left_edge in enumerate(edges):
        left_source = node_lookup.get(left_edge["source"])
        left_target = node_lookup.get(left_edge["target"])

        if not left_source or not left_target:
            continue

        for right_edge in edges[left_index + 1:]:
            right_source = node_lookup.get(right_edge["source"])
            right_target = node_lookup.get(right_edge["target"])

            if not right_source or not right_target:
                continue

            source_diff = left_source["orderScore"] - right_source["orderScore"]
            target_diff = left_target["orderScore"] - right_target["orderScore"]

            if source_diff == 0 or target_diff == 0:
                continue

            if source_diff * target_diff < 0:
                crossing_score += min(left_edge["count"], right_edge["count"])

    return crossing_score


def _count_layer_crossings(layer, edges, node_lookup):
    outgoing_groups = defaultdict(list)
    incoming_groups = defaultdict(list)

    for edge in edges:
        source_node = node_lookup.get(edge["source"])
        target_node = node_lookup.get(edge["target"])

        if not source_node or not target_node:
            continue

        if source_node["layer"] == layer and target_node["layer"] > layer:
            outgoing_groups[target_node["layer"]].append(edge)

        if target_node["layer"] == layer and source_node["layer"] < layer:
            incoming_groups[source_node["layer"]].append(edge)

    return sum(
        _count_edge_crossings(group_edges, node_lookup)
        for group_edges in outgoing_groups.values()
    ) + sum(
        _count_edge_crossings(group_edges, node_lookup)
        for group_edges in incoming_groups.values()
    )


def _optimize_layer_by_swaps(layer_nodes, edges, node_lookup, max_swaps=100):
    if len(layer_nodes) < 2:
        return

    layer = layer_nodes[0]["layer"]
    updated = True
    swap_count = 0

    while updated and swap_count < max_swaps:
        updated = False

        for index in range(len(layer_nodes) - 1):
            current_score = _count_layer_crossings(layer, edges, node_lookup)
            first_node = layer_nodes[index]
            second_node = layer_nodes[index + 1]

            layer_nodes[index], layer_nodes[index + 1] = second_node, first_node
            _reindex_layer_nodes(layer_nodes)

            swapped_score = _count_layer_crossings(layer, edges, node_lookup)
            if swapped_score < current_score:
                updated = True
                swap_count += 1
                if swap_count >= max_swaps:
                    break
                continue

            layer_nodes[index], layer_nodes[index + 1] = first_node, second_node
            _reindex_layer_nodes(layer_nodes)


def _incoming_barycenter(node, edges, node_lookup):
    total_weight = 0
    total_score = 0

    for edge in edges:
        if edge["target"] != node["name"]:
            continue

        source_node = node_lookup.get(edge["source"])
        if not source_node or source_node["layer"] >= node["layer"]:
            continue

        distance = max(1, node["layer"] - source_node["layer"])
        weight = edge["count"] / distance
        total_weight += weight
        total_score += source_node["orderScore"] * weight

    if total_weight == 0:
        return node["orderScore"]

    return total_score / total_weight


def _outgoing_barycenter(node, edges, node_lookup):
    total_weight = 0
    total_score = 0

    for edge in edges:
        if edge["source"] != node["name"]:
            continue

        target_node = node_lookup.get(edge["target"])
        if not target_node or target_node["layer"] <= node["layer"]:
            continue

        distance = max(1, target_node["layer"] - node["layer"])
        weight = edge["count"] / distance
        total_weight += weight
        total_score += target_node["orderScore"] * weight

    if total_weight == 0:
        return node["orderScore"]

    return total_score / total_weight


def _apply_flow_layout(nodes, edges):
    if not nodes:
        return [], []

    edges = sorted(edges, key=lambda edge: (-edge["count"], edge["source"], edge["target"]))

    layer_values = sorted({node["layer"] for node in nodes})
    layer_map = {layer_value: index for index, layer_value in enumerate(layer_values)}
    nodes_by_layer = defaultdict(list)

    for node in nodes:
        node["layer"] = layer_map[node["layer"]]
        nodes_by_layer[node["layer"]].append(node)

    for layer in sorted(nodes_by_layer):
        nodes_by_layer[layer].sort(
            key=lambda node: (node["layerScore"], -node["weight"], node["name"])
        )
        _reindex_layer_nodes(nodes_by_layer[layer])

    node_lookup = {node["name"]: node for node in nodes}
    max_layer = max(nodes_by_layer) if nodes_by_layer else 0

    # Repeat the sweep so dense graphs keep a stable left-to-right order.
    for _ in range(FLOW_LAYOUT_SWEEP_ITERATIONS):
        for layer in range(1, max_layer + 1):
            layer_nodes = nodes_by_layer.get(layer, [])
            layer_nodes.sort(
                key=lambda node: (
                    _incoming_barycenter(node, edges, node_lookup),
                    -node["weight"],
                    node["name"],
                )
            )
            _reindex_layer_nodes(layer_nodes)

        for layer in range(max_layer - 1, -1, -1):
            layer_nodes = nodes_by_layer.get(layer, [])
            layer_nodes.sort(
                key=lambda node: (
                    _outgoing_barycenter(node, edges, node_lookup),
                    -node["weight"],
                    node["name"],
                )
            )
            _reindex_layer_nodes(layer_nodes)

    for layer in range(1, max_layer):
        # Dense graph safety: don't spend too much time on huge layers
        layer_nodes = nodes_by_layer.get(layer, [])
        if len(layer_nodes) > 50:
            continue
        _optimize_layer_by_swaps(layer_nodes, edges, node_lookup, max_swaps=50)

    ordered_nodes = []
    for layer in sorted(nodes_by_layer):
        ordered_nodes.extend(
            sorted(
                nodes_by_layer[layer],
                key=lambda node: (node["orderScore"], -node["weight"], node["name"]),
            )
        )

    return ordered_nodes, edges


# -----------------------------------------------------------------------------
# Process map, variant flow, and pattern detail snapshots
# -----------------------------------------------------------------------------

def create_pattern_flow_snapshot(
    pattern_rows,
    prepared_df=None,
    transition_rows=None,
    frequency_rows=None,
    pattern_percent=10,
    pattern_count=None,
    activity_percent=40,
    connection_percent=30,
    pattern_cap=FLOW_PATTERN_CAP,
):
    frequency_rows = frequency_rows or []
    requested_activity_percent = clamp_flow_percent(activity_percent)
    requested_connection_percent = clamp_flow_percent(connection_percent)
    pattern_selection = select_pattern_rows_for_flow(
        pattern_rows,
        pattern_percent=pattern_percent,
        pattern_count=pattern_count,
        pattern_cap=pattern_cap,
    )
    selected_pattern_rows = pattern_selection["selected_pattern_rows"]
    selected_frequency_rows = list(frequency_rows or [])
    selected_transition_rows = list(transition_rows or [])

    if not selected_transition_rows and prepared_df is not None:
        selected_pattern_df = _build_selected_pattern_prepared_df(prepared_df, selected_pattern_rows)
        if selected_pattern_df is not None and not selected_pattern_df.empty:
            selected_transition_rows = convert_analysis_result_to_records(
                create_transition_analysis(selected_pattern_df),
                TRANSITION_ANALYSIS_CONFIG["display_columns"],
            )

    nodes, edges = _build_flow_graph(
        pattern_rows=selected_pattern_rows,
        transition_rows=selected_transition_rows,
        frequency_rows=selected_frequency_rows,
    )
    filtered_graph = _filter_flow_graph(
        nodes=nodes,
        edges=edges,
        activity_percent=requested_activity_percent,
        connection_percent=requested_connection_percent,
    )

    return {
        "pattern_window": {
            "requested_percent": pattern_selection["requested_percent"],
            "requested_count": pattern_selection["requested_count"],
            "total_pattern_count": pattern_selection["total_pattern_count"],
            "effective_pattern_count": pattern_selection["effective_pattern_count"],
            "used_pattern_count": pattern_selection["used_pattern_count"],
            "cap": pattern_selection["cap"],
        },
        "activity_window": {
            "requested_percent": requested_activity_percent,
            "available_activity_count": filtered_graph["available_activity_count"],
            "visible_activity_count": filtered_graph["visible_activity_count"],
        },
        "connection_window": {
            "requested_percent": requested_connection_percent,
            "available_connection_count": filtered_graph["available_connection_count"],
            "visible_connection_count": filtered_graph["visible_connection_count"],
        },
        "flow_data": {
            "nodes": filtered_graph["nodes"],
            "edges": filtered_graph["edges"],
        },
    }


def create_variant_flow_snapshot(
    prepared_df,
    variant_pattern,
    activity_percent=100,
    connection_percent=100,
):
    filtered_df = filter_prepared_df_by_pattern(prepared_df, variant_pattern)

    if filtered_df.empty:
        raise ValueError("バリアントが見つかりません。")

    return create_pattern_flow_snapshot(
        pattern_rows=[
            {
                FLOW_PATTERN_CASE_COUNT_COLUMN: int(filtered_df["case_id"].nunique()),
                FLOW_PATTERN_COLUMN: variant_pattern,
            }
        ],
        prepared_df=filtered_df,
        pattern_percent=100,
        pattern_count=1,
        activity_percent=activity_percent,
        connection_percent=connection_percent,
        pattern_cap=1,
    )


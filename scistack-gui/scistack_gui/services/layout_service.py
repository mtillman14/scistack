"""
Layout service — single source of truth for layout CRUD operations.

Thin orchestration keeping protocol adapters from importing data access directly.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def get_layout() -> dict:
    from scistack_gui import layout as layout_store
    return layout_store.read_layout()


def put_layout(node_id: str, x: float, y: float,
               node_type: str | None = None, label: str | None = None) -> dict:
    from scistack_gui import layout as layout_store
    if node_type and label:
        if node_type == "functionNode":
            from scistack_gui import matlab_registry
            if matlab_registry.is_matlab_function(label):
                info = matlab_registry.get_matlab_function(label)
                logger.info(
                    "Function node placed: %r (MATLAB, n_outputs=%d, output_names=%s)",
                    label, info.n_outputs, info.output_names,
                )
            else:
                logger.info("Function node placed: %r (Python)", label)
        else:
            logger.debug("Node added to DAG: node_id=%r, type=%r, label=%r, position=(%.1f, %.1f)",
                         node_id, node_type, label, x, y)
        layout_store.write_manual_node(node_id, x, y, node_type, label)
    else:
        layout_store.write_node_position(node_id, x, y)
    return {"ok": True}


def delete_layout(node_id: str) -> dict:
    from scistack_gui import layout as layout_store
    layout_store.delete_node(node_id)
    return {"ok": True}


def put_edge(edge_id: str, source: str, target: str,
             source_handle: str | None = None,
             target_handle: str | None = None) -> dict:
    from scistack_gui import layout as layout_store
    layout_store.write_manual_edge({
        "id": edge_id,
        "source": source,
        "target": target,
        "sourceHandle": source_handle,
        "targetHandle": target_handle,
    })
    return {"ok": True}


def delete_edge(edge_id: str) -> dict:
    from scistack_gui import layout as layout_store
    layout_store.delete_manual_edge(edge_id)
    return {"ok": True}


def get_constants() -> list[str]:
    from scistack_gui import layout as layout_store
    return layout_store.read_all_constant_names()


def create_constant(name: str) -> dict:
    from scistack_gui import layout as layout_store
    logger.debug("Node created (added to palette): type=constant, name=%r", name)
    layout_store.write_constant(name)
    return {"ok": True}


def delete_constant(name: str) -> dict:
    from scistack_gui import layout as layout_store
    layout_store.delete_constant(name)
    return {"ok": True}


def get_path_inputs() -> list[dict]:
    from scistack_gui import layout as layout_store
    return layout_store.read_all_path_input_names()


def create_path_input(name: str, template: str = "",
                      root_folder: str | None = None) -> dict:
    from scistack_gui import layout as layout_store
    logger.debug("Node created (added to palette): type=pathInput, name=%r, template=%r, root_folder=%r",
                 name, template, root_folder)
    layout_store.write_path_input(name, template, root_folder)
    return {"ok": True}


def update_path_input(name: str, template: str = "",
                      root_folder: str | None = None) -> dict:
    from scistack_gui import layout as layout_store
    logger.debug("PathInput updated: name=%r, template=%r, root_folder=%r",
                 name, template, root_folder)
    layout_store.write_path_input(name, template, root_folder)
    return {"ok": True}


def delete_path_input(name: str) -> dict:
    from scistack_gui import layout as layout_store
    layout_store.delete_path_input(name)
    return {"ok": True}


def put_pending_constant(name: str, value: str) -> dict:
    from scistack_gui import layout as layout_store
    layout_store.add_pending_constant(name, value)
    return {"ok": True}


def delete_pending_constant(name: str, value: str) -> dict:
    from scistack_gui import layout as layout_store
    layout_store.remove_pending_constant(name, value)
    return {"ok": True}


def put_node_config(db, node_id: str, config: dict) -> dict:
    from scistack_gui import pipeline_store
    pipeline_store.update_node_config(db, node_id, config)
    return {"ok": True}

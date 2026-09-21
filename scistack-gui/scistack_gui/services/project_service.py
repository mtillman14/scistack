"""
Project service — delegates to api/project.py, which owns the scan.
"""

from __future__ import annotations


def get_project_code() -> dict:
    from scistack_gui.api.project import get_project_code

    return get_project_code()


def refresh_project() -> dict:
    from scistack_gui.api.project import refresh_project_sync

    return refresh_project_sync()


def get_project_paths() -> dict:
    from scistack_gui.api.project import get_project_paths

    return get_project_paths()


def add_project_path(path: str) -> dict:
    from scistack_gui.api.project import add_project_path

    return add_project_path(path)


def remove_project_path(path: str) -> dict:
    from scistack_gui.api.project import remove_project_path

    return remove_project_path(path)


def set_entities_file(path: "str | None") -> dict:
    from scistack_gui.api.project import set_project_entities_file

    return set_project_entities_file(path)


def clear_entities_file() -> dict:
    from scistack_gui.api.project import clear_project_entities_file

    return clear_project_entities_file()

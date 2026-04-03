"""
GET /api/registry

Returns all pipeline functions and variable types known to the server,
sourced from the user module loaded at startup.
"""

from fastapi import APIRouter
from scistack_gui import registry
from scidb import BaseVariable

router = APIRouter()


@router.get("/registry")
def get_registry() -> dict:
    return {
        "functions": sorted(registry._functions.keys()),
        "variables": sorted(BaseVariable._all_subclasses.keys()),
    }
